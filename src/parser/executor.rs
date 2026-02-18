//! AQL Executor - Connects parsed AQL to Aurora operations
//!
//! Provides the bridge between AQL AST and Aurora's database operations.
//! All operations (queries, mutations, subscriptions) go through execute().

use super::ast::{self, Field, Filter as AqlFilter, FragmentDef, MutationOp, Operation, Selection};
use super::executor_utils::{CompiledFilter, compile_filter};

use crate::Aurora;
use crate::error::{AqlError, ErrorCode, Result};
use crate::types::{Document, FieldDefinition, FieldType, ScalarType, Value};
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

pub type ExecutionContext = HashMap<String, JsonValue>;

/// A pre-compiled query plan that can be executed repeatedly with different variables
#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub collection: String,
    pub filter: Option<ast::Filter>,
    pub compiled_filter: Option<CompiledFilter>,
    pub projection: Vec<ast::Field>,
    pub limit: Option<usize>,
    pub offset: usize,
    pub after: Option<String>,
    pub orderings: Vec<ast::Ordering>,
    pub is_connection: bool,
    pub has_lookups: bool,
    pub fragments: HashMap<String, FragmentDef>,
    pub variable_definitions: Vec<ast::VariableDefinition>,
}

impl QueryPlan {
    pub fn validate(&self, provided_variables: &HashMap<String, ast::Value>) -> Result<()> {
        validate_required_variables(&self.variable_definitions, provided_variables)
    }

    pub fn from_query(
        query: &ast::Query,
        fragments: &HashMap<String, FragmentDef>,
    ) -> Result<Vec<Self>> {
        let root_fields = collect_fields(
            &query.selection_set,
            fragments,
            &query.variables_values,
            None,
        )?;

        let mut plans = Vec::new();
        for field in root_fields {
            let collection = field.name.clone();
            let filter = extract_filter_from_args(&field.arguments)?;
            let compiled_filter = if let Some(ref f) = filter {
                Some(compile_filter(f)?)
            } else {
                None
            };

            let sub_fields = collect_fields(
                &field.selection_set,
                fragments,
                &query.variables_values,
                Some(&field.name),
            )?;

            let (limit, offset) = extract_pagination(&field.arguments);
            let (first, after, _last, _before) = extract_cursor_pagination(&field.arguments);
            let orderings = extract_order_by(&field.arguments);
            let is_connection = sub_fields
                .iter()
                .any(|f| f.name == "edges" || f.name == "pageInfo");

            let has_lookups = sub_fields.iter().any(|f| {
                f.arguments.iter().any(|arg| {
                    arg.name == "collection"
                        || arg.name == "localField"
                        || arg.name == "foreignField"
                })
            });

            plans.push(QueryPlan {
                collection,
                filter,
                compiled_filter,
                projection: sub_fields,
                limit: limit.or(first),
                offset,
                after,
                orderings,
                is_connection,
                has_lookups,
                fragments: fragments.clone(),
                variable_definitions: query.variable_definitions.clone(),
            });
        }
        Ok(plans)
    }
}

/// Execute an AQL query string against the database
pub async fn execute(db: &Aurora, aql: &str, options: ExecutionOptions) -> Result<ExecutionResult> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Compute query hash for cache lookup (ignoring whitespace variations)
    let query_key = {
        let mut hasher = DefaultHasher::new();
        aql.trim().hash(&mut hasher);
        hasher.finish()
    };

    // Prepare variables for this execution
    let vars: HashMap<String, ast::Value> = options
        .variables
        .iter()
        .map(|(k, v)| (k.clone(), json_to_aql_value(v.clone())))
        .collect();

    // HIGH-SPEED PATH: Check for pre-compiled Query Plan
    if let Some(plan) = db.plan_cache.get(&query_key) {
        plan.validate(&vars)?;
        return execute_plan(db, &plan, &vars, &options).await;
    }

    // SLOW PATH: Parse and Compile
    let mut doc = super::parse(aql)?;

    // Extract first query operation for planning
    if let Some(Operation::Query(query)) = doc
        .operations
        .iter()
        .find(|op| matches!(op, Operation::Query(_)))
    {
        let fragments: HashMap<String, FragmentDef> = doc
            .operations
            .iter()
            .filter_map(|op| {
                if let Operation::FragmentDefinition(f) = op {
                    Some((f.name.clone(), f.clone()))
                } else {
                    None
                }
            })
            .collect();

        let plans = QueryPlan::from_query(query, &fragments)?;
        if plans.len() == 1 {
            let plan = Arc::new(plans[0].clone());
            plan.validate(&vars)?;
            db.plan_cache.insert(query_key, Arc::clone(&plan));

            return execute_plan(db, &plan, &vars, &options).await;
        }
    }

    // FALLBACK: Normal execution for complex documents (but WITH our prepared vars)
    // We must resolve variables in the AST before executing the document path
    super::validator::resolve_variables(&mut doc, &vars).map_err(|e| {
        let code = match e.code {
            super::validator::ErrorCode::MissingRequiredVariable => ErrorCode::UndefinedVariable,
            super::validator::ErrorCode::TypeMismatch => ErrorCode::TypeError,
            _ => ErrorCode::QueryError,
        };
        AqlError::new(code, e.to_string())
    })?;

    execute_document(db, &doc, &options).await
}

async fn execute_plan(
    db: &Aurora,
    plan: &QueryPlan,
    variables: &HashMap<String, ast::Value>,
    options: &ExecutionOptions,
) -> Result<ExecutionResult> {
    let collection_name = &plan.collection;

    // Determine effective limit for the scan
    let effective_limit = plan.limit;

    // 1. Resolve Indexed Equality (Fast path)
    let mut indexed_docs = None;
    if let Some(ref f) = plan.filter {
        // We need a version of find_indexed that uses our runtime variables
        if let Some((field, val)) =
            find_indexed_equality_filter_runtime(f, db, collection_name, variables)
        {
            let db_val = aql_value_to_db_value(&val, variables)?;
            let ids = db.get_ids_from_index(collection_name, &field, &db_val);

            let mut docs = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some(doc) = db.get_document(collection_name, &id)? {
                    // Check if doc matches the REST of the plan's compiled filter
                    if let Some(ref cf) = plan.compiled_filter {
                        if matches_filter(&doc, cf, variables) {
                            docs.push(doc);
                        }
                    } else {
                        docs.push(doc);
                    }
                }
            }
            indexed_docs = Some(docs);
        }
    }

    let mut docs = if let Some(d) = indexed_docs {
        d
    } else {
        // Fallback to scan
        let vars_arc = Arc::new(variables.clone());
        let cf_clone = plan.compiled_filter.clone();
        let filter_fn = move |doc: &Document| {
            cf_clone
                .as_ref()
                .map(|f| matches_filter(doc, f, &vars_arc))
                .unwrap_or(true)
        };

        let scan_limit = plan.limit.map(|l| {
            let base = if plan.is_connection { l + 1 } else { l };
            base + plan.offset
        });
        db.scan_and_filter(collection_name, filter_fn, scan_limit)?
    };

    // 2. Sort (must happen before offset drain)
    if !plan.orderings.is_empty() {
        apply_ordering(&mut docs, &plan.orderings);
    }

    // 2b. Apply offset
    if plan.offset > 0 {
        if plan.offset < docs.len() {
            docs.drain(0..plan.offset);
        } else {
            docs.clear();
        }
    }

    // 3. Handle Connection vs Flat List
    if plan.is_connection {
        return Ok(ExecutionResult::Query(execute_connection(
            docs,
            &plan.projection,
            effective_limit,
            &plan.fragments,
            variables,
        )));
    }

    // 4. Project (Flat List)
    if options.apply_projections && !plan.projection.is_empty() {
        // Check if we have an aggregate or groupBy field
        let agg_field = plan.projection.iter().find(|f| f.name == "aggregate");
        let group_by_field = plan.projection.iter().find(|f| f.name == "groupBy");

        if let Some(f) = group_by_field {
            // Handle GroupBy
            let field_name = f
                .arguments
                .iter()
                .find(|a| a.name == "field")
                .and_then(|a| match &a.value {
                    ast::Value::String(s) => Some(s),
                    _ => None,
                });

            if let Some(group_field) = field_name {
                let mut groups: HashMap<String, Vec<Document>> = HashMap::new();
                for d in docs {
                    let key = d
                        .data
                        .get(group_field)
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            _ => v.to_string(),
                        })
                        .unwrap_or_else(|| "null".to_string());
                    groups.entry(key).or_default().push(d);
                }

                let mut group_docs = Vec::with_capacity(groups.len());
                for (key, group_items) in groups {
                    let mut data = HashMap::new();
                    for selection in &f.selection_set {
                        if let Selection::Field(sub_f) = selection {
                            let alias = sub_f.alias.as_ref().unwrap_or(&sub_f.name);
                            match sub_f.name.as_str() {
                                "key" => {
                                    data.insert(alias.clone(), Value::String(key.clone()));
                                }
                                "count" => {
                                    data.insert(
                                        alias.clone(),
                                        Value::Int(group_items.len() as i64),
                                    );
                                }
                                "nodes" => {
                                    let sub_fields = collect_fields(
                                        &sub_f.selection_set,
                                        &plan.fragments,
                                        variables,
                                        None,
                                    )
                                    .unwrap_or_default();

                                    let projected_nodes = group_items
                                        .iter()
                                        .map(|d| {
                                            let node_data =
                                                apply_projection(d.clone(), &sub_fields).data;
                                            Value::Object(node_data)
                                        })
                                        .collect();
                                    data.insert(alias.clone(), Value::Array(projected_nodes));
                                }
                                "aggregate" => {
                                    let agg_res =
                                        compute_aggregates(&group_items, &sub_f.selection_set);
                                    data.insert(alias.clone(), Value::Object(agg_res));
                                }
                                _ => {}
                            }
                        }
                    }
                    group_docs.push(Document {
                        id: format!("group:{}", key),
                        data,
                    });
                }
                docs = group_docs;
            }
        } else if let Some(agg) = agg_field {
            // If aggregate is the ONLY field, we return a single summary document
            // If there are other fields, we currently follow the behavior of adding the agg to each doc
            // (Test expectations suggest a single doc if only agg is asked)
            let agg_result = compute_aggregates(&docs, &agg.selection_set);
            let alias = agg.alias.as_ref().unwrap_or(&agg.name);

            if plan.projection.len() == 1 {
                let mut data = HashMap::new();
                data.insert(alias.clone(), Value::Object(agg_result));
                docs = vec![Document {
                    id: "aggregate".to_string(),
                    data,
                }];
            } else {
                // Add aggregate to each document
                docs = docs
                    .into_iter()
                    .map(|mut d| {
                        d.data
                            .insert(alias.clone(), Value::Object(agg_result.clone()));
                        apply_projection(d, &plan.projection)
                    })
                    .collect();
            }
        } else if !plan.has_lookups {
            docs = docs
                .into_iter()
                .map(|d| apply_projection(d, &plan.projection))
                .collect();
        } else {
            // Slow path for lookups
            let fragments = HashMap::new();
            let mut projected = Vec::with_capacity(docs.len());
            for d in docs {
                projected.push(
                    apply_projection_with_lookups(
                        db,
                        d,
                        &[],
                        variables,
                        &fragments,
                        collection_name,
                    )
                    .await?,
                );
            }
            docs = projected;
        }
    }

    Ok(ExecutionResult::Query(QueryResult {
        collection: collection_name.clone(),
        documents: docs,
        total_count: None,
    }))
}

fn compute_aggregates(docs: &[Document], selections: &[Selection]) -> HashMap<String, Value> {
    let mut results = HashMap::new();

    for selection in selections {
        if let Selection::Field(f) = selection {
            let alias = f.alias.as_ref().unwrap_or(&f.name);
            let value = match f.name.as_str() {
                "count" => Value::Int(docs.len() as i64),
                "sum" => {
                    let field =
                        f.arguments
                            .iter()
                            .find(|a| a.name == "field")
                            .and_then(|a| match &a.value {
                                ast::Value::String(s) => Some(s),
                                _ => None,
                            });

                    if let Some(field_name) = field {
                        let sum: f64 = docs
                            .iter()
                            .filter_map(|d| d.data.get(field_name))
                            .filter_map(|v| match v {
                                Value::Int(i) => Some(*i as f64),
                                Value::Float(f) => Some(*f),
                                _ => None,
                            })
                            .sum();
                        Value::Float(sum)
                    } else {
                        Value::Null
                    }
                }
                "avg" => {
                    let field =
                        f.arguments
                            .iter()
                            .find(|a| a.name == "field")
                            .and_then(|a| match &a.value {
                                ast::Value::String(s) => Some(s),
                                _ => None,
                            });

                    if let Some(field_name) = field
                        && !docs.is_empty()
                    {
                        let values: Vec<f64> = docs
                            .iter()
                            .filter_map(|d| d.data.get(field_name))
                            .filter_map(|v| match v {
                                Value::Int(i) => Some(*i as f64),
                                Value::Float(f) => Some(*f),
                                _ => None,
                            })
                            .collect();

                        if values.is_empty() {
                            Value::Null
                        } else {
                            let sum: f64 = values.iter().sum();
                            Value::Float(sum / values.len() as f64)
                        }
                    } else {
                        Value::Null
                    }
                }
                "min" => {
                    let field =
                        f.arguments
                            .iter()
                            .find(|a| a.name == "field")
                            .and_then(|a| match &a.value {
                                ast::Value::String(s) => Some(s),
                                _ => None,
                            });

                    if let Some(field_name) = field
                        && !docs.is_empty()
                    {
                        let min = docs
                            .iter()
                            .filter_map(|d| d.data.get(field_name))
                            .filter_map(|v| match v {
                                Value::Int(i) => Some(*i as f64),
                                Value::Float(f) => Some(*f),
                                _ => None,
                            })
                            .fold(f64::INFINITY, f64::min);

                        if min == f64::INFINITY {
                            Value::Null
                        } else {
                            Value::Float(min)
                        }
                    } else {
                        Value::Null
                    }
                }
                "max" => {
                    let field =
                        f.arguments
                            .iter()
                            .find(|a| a.name == "field")
                            .and_then(|a| match &a.value {
                                ast::Value::String(s) => Some(s),
                                _ => None,
                            });

                    if let Some(field_name) = field
                        && !docs.is_empty()
                    {
                        let max = docs
                            .iter()
                            .filter_map(|d| d.data.get(field_name))
                            .filter_map(|v| match v {
                                Value::Int(i) => Some(*i as f64),
                                Value::Float(f) => Some(*f),
                                _ => None,
                            })
                            .fold(f64::NEG_INFINITY, f64::max);

                        if max == f64::NEG_INFINITY {
                            Value::Null
                        } else {
                            Value::Float(max)
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };
            results.insert(alias.clone(), value);
        }
    }

    results
}

fn find_indexed_equality_filter_runtime(
    filter: &ast::Filter,
    db: &Aurora,
    collection: &str,
    variables: &HashMap<String, ast::Value>,
) -> Option<(String, ast::Value)> {
    match filter {
        ast::Filter::Eq(field, val) => {
            if field == "id" || db.has_index(collection, field) {
                let resolved = resolve_if_variable(val, variables);
                return Some((field.clone(), resolved.clone()));
            }
        }
        ast::Filter::And(filters) => {
            for f in filters {
                if let Some(res) =
                    find_indexed_equality_filter_runtime(f, db, collection, variables)
                {
                    return Some(res);
                }
            }
        }
        _ => {}
    }
    None
}

/// Helper to flatten a selection set (processing fragments) into a list of fields
fn collect_fields(
    selection_set: &[Selection],
    fragments: &HashMap<String, FragmentDef>,
    variable_values: &HashMap<String, ast::Value>,
    parent_type: Option<&str>,
) -> Result<Vec<Field>> {
    let mut fields = Vec::new();

    for selection in selection_set {
        match selection {
            Selection::Field(field) => {
                if should_include(&field.directives, variable_values)? {
                    fields.push(field.clone());
                }
            }
            Selection::FragmentSpread(name) => {
                if let Some(fragment) = fragments.get(name) {
                    let type_match = if let Some(parent) = parent_type {
                        parent == fragment.type_condition
                    } else {
                        true
                    };

                    if type_match {
                        let fragment_fields = collect_fields(
                            &fragment.selection_set,
                            fragments,
                            variable_values,
                            parent_type,
                        )?;
                        fields.extend(fragment_fields);
                    }
                }
            }
            Selection::InlineFragment(inline) => {
                let type_match = if let Some(parent) = parent_type {
                    parent == inline.type_condition
                } else {
                    true
                };

                if type_match {
                    let inline_fields = collect_fields(
                        &inline.selection_set,
                        fragments,
                        variable_values,
                        parent_type,
                    )?;
                    fields.extend(inline_fields);
                }
            }
        }
    }

    Ok(fields)
}

/// Check if a field/fragment should be included based on @skip/@include
fn should_include(
    directives: &[ast::Directive],
    variables: &HashMap<String, ast::Value>,
) -> Result<bool> {
    for dir in directives {
        if dir.name == "skip" {
            if let Some(arg) = dir.arguments.iter().find(|a| a.name == "if") {
                let should_skip = resolve_boolean_arg(&arg.value, variables)?;
                if should_skip {
                    return Ok(false);
                }
            }
        } else if dir.name == "include" {
            if let Some(arg) = dir.arguments.iter().find(|a| a.name == "if") {
                let should_include = resolve_boolean_arg(&arg.value, variables)?;
                if !should_include {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

fn resolve_boolean_arg(
    value: &ast::Value,
    variables: &HashMap<String, ast::Value>,
) -> Result<bool> {
    match value {
        ast::Value::Boolean(b) => Ok(*b),
        ast::Value::Variable(name) => {
            if let Some(val) = variables.get(name) {
                match val {
                    ast::Value::Boolean(b) => Ok(*b),
                    _ => Err(AqlError::new(
                        ErrorCode::TypeError,
                        format!("Variable '{}' is not a boolean, got {:?}", name, val),
                    )),
                }
            } else {
                Err(AqlError::new(
                    ErrorCode::UndefinedVariable,
                    format!("Variable '{}' is not defined", name),
                ))
            }
        }
        _ => Err(AqlError::new(
            ErrorCode::TypeError,
            format!("Expected boolean value, got {:?}", value),
        )),
    }
}

/// Validate that all required variables are provided
fn validate_required_variables(
    variable_definitions: &[ast::VariableDefinition],
    provided_variables: &HashMap<String, ast::Value>,
) -> Result<()> {
    for var_def in variable_definitions {
        if var_def.var_type.is_required {
            if !provided_variables.contains_key(&var_def.name) {
                if var_def.default_value.is_none() {
                    return Err(AqlError::new(
                        ErrorCode::UndefinedVariable,
                        format!(
                            "Required variable '{}' (type: {}{}) is not provided",
                            var_def.name,
                            var_def.var_type.name,
                            if var_def.var_type.is_required {
                                "!"
                            } else {
                                ""
                            }
                        ),
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Result of executing an AQL operation
#[derive(Debug)]
pub enum ExecutionResult {
    Query(QueryResult),
    Mutation(MutationResult),
    Subscription(SubscriptionResult),
    Batch(Vec<ExecutionResult>),
    Schema(SchemaResult),
    Migration(MigrationResult),
}

#[derive(Debug, Clone)]
pub struct SchemaResult {
    pub operation: String,
    pub collection: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct MigrationResult {
    pub version: String,
    pub steps_applied: usize,
    pub status: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionPlan {
    pub operations: Vec<String>,
    pub estimated_cost: f64,
}

/// Query execution result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub collection: String,
    pub documents: Vec<Document>,
    pub total_count: Option<usize>,
}

/// Mutation execution result
#[derive(Debug, Clone)]
pub struct MutationResult {
    pub operation: String,
    pub collection: String,
    pub affected_count: usize,
    pub returned_documents: Vec<Document>,
}

/// Subscription result
#[derive(Debug)]
pub struct SubscriptionResult {
    pub subscription_id: String,
    pub collection: String,
    pub stream: Option<crate::pubsub::ChangeListener>,
}

/// Execution options
#[derive(Debug, Clone)]
pub struct ExecutionOptions {
    pub skip_validation: bool,
    pub apply_projections: bool,
    pub variables: HashMap<String, JsonValue>,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        Self {
            skip_validation: false,
            apply_projections: true,
            variables: HashMap::new(),
        }
    }
}

impl ExecutionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_variables(mut self, vars: HashMap<String, JsonValue>) -> Self {
        self.variables = vars;
        self
    }

    pub fn skip_validation(mut self) -> Self {
        self.skip_validation = true;
        self
    }
}

fn json_to_aql_value(v: serde_json::Value) -> ast::Value {
    match v {
        serde_json::Value::Null => ast::Value::Null,
        serde_json::Value::Bool(b) => ast::Value::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ast::Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                ast::Value::Float(f)
            } else {
                ast::Value::Null
            }
        }
        serde_json::Value::String(s) => ast::Value::String(s),
        serde_json::Value::Array(arr) => {
            ast::Value::Array(arr.into_iter().map(json_to_aql_value).collect())
        }
        serde_json::Value::Object(map) => ast::Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, json_to_aql_value(v)))
                .collect(),
        ),
    }
}

/// Execute a parsed AQL document
pub async fn execute_document(
    db: &Aurora,
    doc: &ast::Document,
    options: &ExecutionOptions,
) -> Result<ExecutionResult> {
    if doc.operations.is_empty() {
        return Err(AqlError::new(
            ErrorCode::QueryError,
            "No operations in document".to_string(),
        ));
    }

    let vars: HashMap<String, ast::Value> = options
        .variables
        .iter()
        .map(|(k, v)| (k.clone(), json_to_aql_value(v.clone())))
        .collect();

    let fragments: HashMap<String, FragmentDef> = doc
        .operations
        .iter()
        .filter_map(|op| {
            if let Operation::FragmentDefinition(frag) = op {
                Some((frag.name.clone(), frag.clone()))
            } else {
                None
            }
        })
        .collect();

    let executable_ops: Vec<&Operation> = doc
        .operations
        .iter()
        .filter(|op| !matches!(op, Operation::FragmentDefinition(_)))
        .collect();

    if executable_ops.is_empty() {
        return Err(AqlError::new(
            ErrorCode::QueryError,
            "No executable operations in document".to_string(),
        ));
    }

    if executable_ops.len() == 1 {
        execute_operation(db, executable_ops[0], &vars, options, &fragments).await
    } else {
        let mut results = Vec::new();
        for op in executable_ops {
            results.push(execute_operation(db, op, &vars, options, &fragments).await?);
        }
        Ok(ExecutionResult::Batch(results))
    }
}

async fn execute_operation(
    db: &Aurora,
    op: &Operation,
    vars: &HashMap<String, ast::Value>,
    options: &ExecutionOptions,
    fragments: &HashMap<String, FragmentDef>,
) -> Result<ExecutionResult> {
    match op {
        Operation::Query(query) => execute_query(db, query, vars, options, fragments).await,
        Operation::Mutation(mutation) => {
            execute_mutation(db, mutation, vars, options, fragments).await
        }
        Operation::Subscription(sub) => execute_subscription(db, sub, vars, options).await,
        Operation::Schema(schema) => execute_schema(db, schema, options).await,
        Operation::Migration(migration) => execute_migration(db, migration, options).await,
        Operation::Introspection(intro) => execute_introspection(db, intro).await,
        _ => Ok(ExecutionResult::Query(QueryResult {
            collection: String::new(),
            documents: vec![],
            total_count: None,
        })),
    }
}

async fn execute_query(
    db: &Aurora,
    query: &ast::Query,
    vars: &HashMap<String, ast::Value>,
    options: &ExecutionOptions,
    fragments: &HashMap<String, FragmentDef>,
) -> Result<ExecutionResult> {
    validate_required_variables(&query.variable_definitions, vars)?;
    let root_fields = collect_fields(&query.selection_set, fragments, vars, None)?;
    let mut results = Vec::new();
    for field in &root_fields {
        let sub_fields = collect_fields(&field.selection_set, fragments, vars, Some(&field.name))?;
        let result =
            execute_collection_query(db, field, &sub_fields, vars, options, fragments).await?;
        results.push(result);
    }
    if results.len() == 1 {
        Ok(ExecutionResult::Query(results.remove(0)))
    } else {
        Ok(ExecutionResult::Batch(
            results.into_iter().map(ExecutionResult::Query).collect(),
        ))
    }
}

async fn execute_collection_query(
    db: &Aurora,
    field: &ast::Field,
    sub_fields: &[ast::Field],
    variables: &HashMap<String, ast::Value>,
    options: &ExecutionOptions,
    fragments: &HashMap<String, FragmentDef>,
) -> Result<QueryResult> {
    let collection_name = &field.name;
    let filter = extract_filter_from_args(&field.arguments)?;
    let (limit, offset) = extract_pagination(&field.arguments);
    let (first, after, _last, _before) = extract_cursor_pagination(&field.arguments);
    let compiled_filter = if let Some(ref f) = filter {
        Some(compile_filter(f)?)
    } else {
        None
    };
    let vars_arc = Arc::new(variables.clone());
    let filter_fn = move |doc: &Document| {
        compiled_filter
            .as_ref()
            .map(|f| matches_filter(doc, f, &vars_arc))
            .unwrap_or(true)
    };

    let indexed_docs = if let Some(ref f) = filter {
        match find_indexed_equality_filter(f, db, collection_name) {
            Some((field_name, val)) => {
                let db_val = aql_value_to_db_value(&val, variables)?;
                let ids = db.get_ids_from_index(collection_name, &field_name, &db_val);
                let mut docs = Vec::with_capacity(ids.len());
                for id in ids {
                    if let Some(doc) = db.get_document(collection_name, &id)? {
                        if filter_fn(&doc) {
                            docs.push(doc);
                        }
                    }
                }
                Some(docs)
            }
            None => None,
        }
    } else {
        None
    };

    let is_connection = sub_fields
        .iter()
        .any(|f| f.name == "edges" || f.name == "pageInfo");

    let mut docs = if let Some(docs) = indexed_docs {
        docs
    } else {
        let scan_limit = limit
            .or(first)
            .map(|l| {
                let base = if is_connection { l + 1 } else { l };
                base + offset
            });
        db.scan_and_filter(collection_name, filter_fn, scan_limit)?
    };

    // Sort before any pagination so offset/cursor operate on ordered results.
    let orderings = extract_order_by(&field.arguments);
    if !orderings.is_empty() {
        apply_ordering(&mut docs, &orderings);
    }

    // Cursor pagination: skip past the `after` cursor before slicing
    if let Some(ref cursor) = after {
        if let Some(pos) = docs.iter().position(|d| &d.id == cursor) {
            docs.drain(0..=pos);
        }
    }

    if is_connection {
        return Ok(execute_connection(
            docs,
            sub_fields,
            limit.or(first),
            fragments,
            variables,
        ));
    }

    // Offset pagination: skip the first `offset` documents
    if offset > 0 {
        if offset < docs.len() {
            docs.drain(0..offset);
        } else {
            docs.clear();
        }
    }

    if options.apply_projections && !sub_fields.is_empty() {
        docs = docs
            .into_iter()
            .map(|d| apply_projection(d, sub_fields))
            .collect();
    }

    Ok(QueryResult {
        collection: collection_name.clone(),
        documents: docs,
        total_count: None,
    })
}

async fn execute_mutation(
    db: &Aurora,
    mutation: &ast::Mutation,
    vars: &HashMap<String, ast::Value>,
    options: &ExecutionOptions,
    fragments: &HashMap<String, FragmentDef>,
) -> Result<ExecutionResult> {
    use crate::transaction::ACTIVE_TRANSACTION_ID;

    validate_required_variables(&mutation.variable_definitions, vars)?;

    // Wrap every mutation block in a transaction so that a failure in any
    // operation rolls back all previous operations in the same block.
    // put() and aql_delete_document() both check ACTIVE_TRANSACTION_ID, so
    // all writes inside the scope are buffered until commit.
    let tx_id = db.begin_transaction().await;

    let exec_result = ACTIVE_TRANSACTION_ID
        .scope(tx_id, async {
            let mut results = Vec::new();
            let mut context = HashMap::new();
            for mut_op in &mutation.operations {
                let res =
                    execute_mutation_op(db, mut_op, vars, &context, options, fragments).await?;
                if let Some(alias) = &mut_op.alias {
                    if let Some(doc) = res.returned_documents.first() {
                        let mut m = serde_json::Map::new();
                        for (k, v) in &doc.data {
                            m.insert(k.clone(), aurora_value_to_json_value(v));
                        }
                        m.insert("id".to_string(), JsonValue::String(doc.id.clone()));
                        context.insert(alias.clone(), JsonValue::Object(m));
                    }
                }
                results.push(res);
            }
            Ok::<_, crate::error::AqlError>(results)
        })
        .await;

    match exec_result {
        Ok(mut results) => {
            db.commit_transaction(tx_id).await?;
            if results.len() == 1 {
                Ok(ExecutionResult::Mutation(results.remove(0)))
            } else {
                Ok(ExecutionResult::Batch(
                    results.into_iter().map(ExecutionResult::Mutation).collect(),
                ))
            }
        }
        Err(e) => {
            // Best-effort rollback — if it fails the buffer is dropped anyway
            let _ = db.rollback_transaction(tx_id).await;
            Err(e)
        }
    }
}

fn execute_mutation_op<'a>(
    db: &'a Aurora,
    mut_op: &'a ast::MutationOperation,
    variables: &'a HashMap<String, ast::Value>,
    context: &'a ExecutionContext,
    options: &'a ExecutionOptions,
    fragments: &'a HashMap<String, FragmentDef>,
) -> futures::future::BoxFuture<'a, Result<MutationResult>> {
    use futures::future::FutureExt;
    async move {
        match &mut_op.operation {
            MutationOp::Insert { collection, data } => {
                let resolved = resolve_value(data, variables, context);
                let doc = db
                    .aql_insert(collection, aql_value_to_hashmap(&resolved, variables)?)
                    .await?;
                let returned = if !mut_op.selection_set.is_empty() && options.apply_projections {
                    let fields = collect_fields(
                        &mut_op.selection_set,
                        fragments,
                        variables,
                        Some(collection),
                    )
                    .unwrap_or_default();
                    vec![apply_projection(doc, &fields)]
                } else {
                    vec![doc]
                };
                Ok(MutationResult {
                    operation: "insert".to_string(),
                    collection: collection.clone(),
                    affected_count: 1,
                    returned_documents: returned,
                })
            }
            MutationOp::Update {
                collection,
                filter,
                data,
            } => {
                let cf = if let Some(f) = filter {
                    Some(compile_filter(f)?)
                } else {
                    None
                };
                let update_data =
                    aql_value_to_hashmap(&resolve_value(data, variables, context), variables)?;

                // Scan and update matching documents
                let mut affected = 0;
                let mut returned = Vec::new();

                // For now, we use a simple scan-and-update.
                // In production, we'd use indices if the filter allows.
                let vars_arc = Arc::new(variables.clone());
                let cf_arc = cf.map(Arc::new);

                let matches = db.scan_and_filter(
                    collection,
                    |doc| {
                        if let Some(ref filter) = cf_arc {
                            matches_filter(doc, filter, &vars_arc)
                        } else {
                            true
                        }
                    },
                    None,
                )?;

                let fields = if !mut_op.selection_set.is_empty() {
                    Some(
                        collect_fields(
                            &mut_op.selection_set,
                            fragments,
                            variables,
                            Some(collection),
                        )
                        .unwrap_or_default(),
                    )
                } else {
                    None
                };

                for doc in matches {
                    let mut new_data = doc.data.clone();
                    for (k, v) in &update_data {
                        new_data.insert(k.clone(), v.clone());
                    }

                    let updated_doc = db
                        .aql_update_document(collection, &doc.id, new_data)
                        .await?;

                    affected += 1;
                    if let Some(ref f) = fields {
                        returned.push(apply_projection(updated_doc, f));
                    }
                }

                Ok(MutationResult {
                    operation: "update".to_string(),
                    collection: collection.clone(),
                    affected_count: affected,
                    returned_documents: returned,
                })
            }
            MutationOp::Delete { collection, filter } => {
                let cf = if let Some(f) = filter {
                    Some(compile_filter(f)?)
                } else {
                    None
                };

                let mut affected = 0;
                let vars_arc = Arc::new(variables.clone());
                let cf_arc = cf.map(Arc::new);

                let matches = db.scan_and_filter(
                    collection,
                    |doc| {
                        if let Some(ref filter) = cf_arc {
                            matches_filter(doc, filter, &vars_arc)
                        } else {
                            true
                        }
                    },
                    None,
                )?;

                for doc in matches {
                    db.aql_delete_document(collection, &doc.id).await?;
                    affected += 1;
                }

                Ok(MutationResult {
                    operation: "delete".to_string(),
                    collection: collection.clone(),
                    affected_count: affected,
                    returned_documents: vec![],
                })
            }
            _ => Err(AqlError::new(
                ErrorCode::QueryError,
                "Operation not implemented".to_string(),
            )),
        }
    }
    .boxed()
}

async fn execute_subscription(
    db: &Aurora,
    sub: &ast::Subscription,
    vars: &HashMap<String, ast::Value>,
    _options: &ExecutionOptions,
) -> Result<ExecutionResult> {
    let vars: HashMap<String, ast::Value> = vars.clone();

    let selection = sub.selection_set.first().ok_or_else(|| {
        AqlError::new(
            ErrorCode::QueryError,
            "Subscription must have a selection".to_string(),
        )
    })?;

    if let Selection::Field(f) = selection {
        let collection = f.name.clone();
        let filter = extract_filter_from_args(&f.arguments)?;

        let mut listener = db.pubsub.listen(&collection);

        if let Some(f) = filter {
            let event_filter = ast_filter_to_event_filter(&f, &vars)?;
            listener = listener.filter(event_filter);
        }

        Ok(ExecutionResult::Subscription(SubscriptionResult {
            subscription_id: uuid::Uuid::new_v4().to_string(),
            collection,
            stream: Some(listener),
        }))
    } else {
        Err(AqlError::new(
            ErrorCode::QueryError,
            "Invalid subscription selection".to_string(),
        ))
    }
}

fn ast_filter_to_event_filter(
    filter: &AqlFilter,
    vars: &HashMap<String, ast::Value>,
) -> Result<crate::pubsub::EventFilter> {
    use crate::pubsub::EventFilter;

    match filter {
        AqlFilter::Eq(f, v) => Ok(EventFilter::FieldEquals(
            f.clone(),
            aql_value_to_db_value(v, vars)?,
        )),
        AqlFilter::Ne(f, v) => Ok(EventFilter::Ne(f.clone(), aql_value_to_db_value(v, vars)?)),
        AqlFilter::Gt(f, v) => Ok(EventFilter::Gt(f.clone(), aql_value_to_db_value(v, vars)?)),
        AqlFilter::Gte(f, v) => Ok(EventFilter::Gte(f.clone(), aql_value_to_db_value(v, vars)?)),
        AqlFilter::Lt(f, v) => Ok(EventFilter::Lt(f.clone(), aql_value_to_db_value(v, vars)?)),
        AqlFilter::Lte(f, v) => Ok(EventFilter::Lte(f.clone(), aql_value_to_db_value(v, vars)?)),
        AqlFilter::In(f, v) => Ok(EventFilter::In(f.clone(), aql_value_to_db_value(v, vars)?)),
        AqlFilter::NotIn(f, v) => Ok(EventFilter::NotIn(
            f.clone(),
            aql_value_to_db_value(v, vars)?,
        )),
        AqlFilter::Contains(f, v) => Ok(EventFilter::Contains(
            f.clone(),
            aql_value_to_db_value(v, vars)?,
        )),
        AqlFilter::StartsWith(f, v) => Ok(EventFilter::StartsWith(
            f.clone(),
            aql_value_to_db_value(v, vars)?,
        )),
        AqlFilter::EndsWith(f, v) => Ok(EventFilter::EndsWith(
            f.clone(),
            aql_value_to_db_value(v, vars)?,
        )),
        AqlFilter::Matches(f, v) => Ok(EventFilter::Matches(
            f.clone(),
            aql_value_to_db_value(v, vars)?,
        )),
        AqlFilter::IsNull(f) => Ok(EventFilter::IsNull(f.clone())),
        AqlFilter::IsNotNull(f) => Ok(EventFilter::IsNotNull(f.clone())),
        AqlFilter::And(filters) => {
            let mut mapped = Vec::new();
            for f in filters {
                mapped.push(ast_filter_to_event_filter(f, vars)?);
            }
            Ok(EventFilter::And(mapped))
        }
        AqlFilter::Or(filters) => {
            let mut mapped = Vec::new();
            for f in filters {
                mapped.push(ast_filter_to_event_filter(f, vars)?);
            }
            Ok(EventFilter::Or(mapped))
        }
        AqlFilter::Not(f) => Ok(EventFilter::Not(Box::new(ast_filter_to_event_filter(
            f, vars,
        )?))),
    }
}

async fn execute_introspection(
    _db: &Aurora,
    _intro: &ast::IntrospectionQuery,
) -> Result<ExecutionResult> {
    Ok(ExecutionResult::Query(QueryResult {
        collection: "__schema".to_string(),
        documents: vec![],
        total_count: Some(0),
    }))
}

async fn execute_schema(
    db: &Aurora,
    schema: &ast::Schema,
    _options: &ExecutionOptions,
) -> Result<ExecutionResult> {
    let mut last_collection = String::new();

    for op in &schema.operations {
        match op {
            ast::SchemaOp::DefineCollection {
                name,
                fields,
                if_not_exists,
                ..
            } => {
                last_collection = name.clone();

                // If if_not_exists is true, check if it already exists
                if *if_not_exists {
                    if db.get_collection_definition(name).is_ok() {
                        continue;
                    }
                }

                let mut field_defs = Vec::new();
                for field in fields {
                    let field_type = map_ast_type(&field.field_type);

                    let mut indexed = false;
                    let mut unique = false;

                    for directive in &field.directives {
                        if directive.name == "indexed" || directive.name == "index" {
                            indexed = true;
                        } else if directive.name == "unique" {
                            unique = true;
                        }
                    }

                    let def = FieldDefinition {
                        field_type,
                        unique,
                        indexed,
                        nullable: !field.field_type.is_required,
                    };

                    field_defs.push((field.name.as_str(), def));
                }

                db.new_collection(name, field_defs).await?;
            }
            _ => {
                return Err(AqlError::new(
                    ErrorCode::InvalidOperation,
                    "Only DefineCollection is supported in schema blocks for now".to_string(),
                ));
            }
        }
    }

    Ok(ExecutionResult::Schema(SchemaResult {
        operation: "schema".to_string(),
        collection: last_collection,
        status: "done".to_string(),
    }))
}

fn map_ast_type(anno: &ast::TypeAnnotation) -> FieldType {
    let scalar = match anno.name.to_lowercase().as_str() {
        "string" => ScalarType::String,
        "int" | "integer" => ScalarType::Int,
        "float" | "double" => ScalarType::Float,
        "bool" | "boolean" => ScalarType::Bool,
        "uuid" => ScalarType::Uuid,
        "object" => ScalarType::Object,
        "array" => ScalarType::Array,
        _ => ScalarType::Any,
    };

    if anno.is_array {
        FieldType::Array(scalar)
    } else {
        match scalar {
            ScalarType::Object => FieldType::Object,
            ScalarType::Any => FieldType::Any,
            _ => FieldType::Scalar(scalar),
        }
    }
}

async fn execute_migration(
    _db: &Aurora,
    _migration: &ast::Migration,
    _options: &ExecutionOptions,
) -> Result<ExecutionResult> {
    Ok(ExecutionResult::Migration(MigrationResult {
        version: "1.0".to_string(),
        steps_applied: 0,
        status: "applied".to_string(),
    }))
}

fn extract_filter_from_args(args: &[ast::Argument]) -> Result<Option<AqlFilter>> {
    for a in args {
        if a.name == "where" || a.name == "filter" {
            return Ok(Some(value_to_filter(&a.value)?));
        }
    }
    Ok(None)
}

fn extract_order_by(args: &[ast::Argument]) -> Vec<ast::Ordering> {
    let mut orderings = Vec::new();
    for a in args {
        if a.name == "orderBy" {
            match &a.value {
                ast::Value::String(f) => orderings.push(ast::Ordering {
                    field: f.clone(),
                    direction: ast::SortDirection::Asc,
                }),
                ast::Value::Object(obj) => {
                    for (field, dir_val) in obj {
                        let direction = match dir_val {
                            ast::Value::Enum(s) | ast::Value::String(s) => {
                                if s.to_uppercase() == "DESC" {
                                    ast::SortDirection::Desc
                                } else {
                                    ast::SortDirection::Asc
                                }
                            }
                            _ => ast::SortDirection::Asc,
                        };
                        orderings.push(ast::Ordering {
                            field: field.clone(),
                            direction,
                        });
                    }
                }
                _ => {}
            }
        }
    }
    orderings
}

fn apply_ordering(docs: &mut [Document], orderings: &[ast::Ordering]) {
    docs.sort_by(|a, b| {
        for o in orderings {
            let cmp = compare_values(a.data.get(&o.field), b.data.get(&o.field));
            if cmp != std::cmp::Ordering::Equal {
                return match o.direction {
                    ast::SortDirection::Asc => cmp,
                    ast::SortDirection::Desc => cmp.reverse(),
                };
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(av), Some(bv)) => av.partial_cmp(bv).unwrap_or(std::cmp::Ordering::Equal),
    }
}

pub fn extract_pagination(args: &[ast::Argument]) -> (Option<usize>, usize) {
    let (mut limit, mut offset) = (None, 0);
    for a in args {
        match a.name.as_str() {
            "limit" | "first" => {
                if let ast::Value::Int(n) = a.value {
                    limit = Some(n as usize);
                }
            }
            "offset" | "skip" => {
                if let ast::Value::Int(n) = a.value {
                    offset = n as usize;
                }
            }
            _ => {}
        }
    }
    (limit, offset)
}

fn extract_cursor_pagination(
    args: &[ast::Argument],
) -> (Option<usize>, Option<String>, Option<usize>, Option<String>) {
    let (mut first, mut after, mut last, mut before) = (None, None, None, None);
    for a in args {
        match a.name.as_str() {
            "first" => {
                if let ast::Value::Int(n) = a.value {
                    first = Some(n as usize);
                }
            }
            "after" => {
                if let ast::Value::String(s) = &a.value {
                    after = Some(s.clone());
                }
            }
            "last" => {
                if let ast::Value::Int(n) = a.value {
                    last = Some(n as usize);
                }
            }
            "before" => {
                if let ast::Value::String(s) = &a.value {
                    before = Some(s.clone());
                }
            }
            _ => {}
        }
    }
    (first, after, last, before)
}

fn execute_connection(
    mut docs: Vec<Document>,
    sub_fields: &[ast::Field],
    limit: Option<usize>,
    fragments: &HashMap<String, FragmentDef>,
    variables: &HashMap<String, ast::Value>,
) -> QueryResult {
    let has_next_page = if let Some(l) = limit {
        docs.len() > l
    } else {
        false
    };

    if has_next_page {
        docs.truncate(limit.unwrap());
    }

    let mut edges = Vec::with_capacity(docs.len());
    let mut end_cursor = String::new();

    // Find the 'node' selection fields once (potentially expanding fragments)
    let node_fields = if let Some(edges_field) = sub_fields.iter().find(|f| f.name == "edges") {
        let edges_sub_fields =
            collect_fields(&edges_field.selection_set, fragments, variables, None)
                .unwrap_or_default();
        if let Some(node_field) = edges_sub_fields.into_iter().find(|f| f.name == "node") {
            collect_fields(&node_field.selection_set, fragments, variables, None)
                .unwrap_or_default()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    for doc in &docs {
        let cursor = doc.id.clone();
        end_cursor = cursor.clone();

        let mut edge_data = HashMap::new();
        edge_data.insert("cursor".to_string(), Value::String(cursor));

        let node_doc = if node_fields.is_empty() {
            doc.clone()
        } else {
            apply_projection(doc.clone(), &node_fields)
        };

        edge_data.insert("node".to_string(), Value::Object(node_doc.data));
        edges.push(Value::Object(edge_data));
    }

    let mut page_info = HashMap::new();
    page_info.insert("hasNextPage".to_string(), Value::Bool(has_next_page));
    page_info.insert("endCursor".to_string(), Value::String(end_cursor));

    let mut conn_data = HashMap::new();
    conn_data.insert("edges".to_string(), Value::Array(edges));
    conn_data.insert("pageInfo".to_string(), Value::Object(page_info));

    QueryResult {
        collection: String::new(),
        documents: vec![Document {
            id: "connection".to_string(),
            data: conn_data,
        }],
        total_count: None,
    }
}

pub fn matches_filter(
    doc: &Document,
    filter: &CompiledFilter,
    vars: &HashMap<String, ast::Value>,
) -> bool {
    match filter {
        CompiledFilter::Eq(f, v) => doc
            .data
            .get(f)
            .map_or(false, |dv| values_equal(dv, v, vars)),
        CompiledFilter::Ne(f, v) => doc
            .data
            .get(f)
            .map_or(true, |dv| !values_equal(dv, v, vars)),
        CompiledFilter::Gt(f, v) => doc.data.get(f).map_or(false, |dv| {
            if let Ok(bv) = aql_value_to_db_value(v, vars) {
                return dv > &bv;
            }
            false
        }),
        CompiledFilter::Gte(f, v) => doc.data.get(f).map_or(false, |dv| {
            if let Ok(bv) = aql_value_to_db_value(v, vars) {
                return dv >= &bv;
            }
            false
        }),
        CompiledFilter::Lt(f, v) => doc.data.get(f).map_or(false, |dv| {
            if let Ok(bv) = aql_value_to_db_value(v, vars) {
                return dv < &bv;
            }
            false
        }),
        CompiledFilter::Lte(f, v) => doc.data.get(f).map_or(false, |dv| {
            if let Ok(bv) = aql_value_to_db_value(v, vars) {
                return dv <= &bv;
            }
            false
        }),
        CompiledFilter::In(f, v) => doc.data.get(f).map_or(false, |dv| {
            if let Ok(Value::Array(arr)) = aql_value_to_db_value(v, vars) {
                return arr.contains(dv);
            }
            false
        }),
        CompiledFilter::NotIn(f, v) => doc.data.get(f).map_or(true, |dv| {
            if let Ok(Value::Array(arr)) = aql_value_to_db_value(v, vars) {
                return !arr.contains(dv);
            }
            true
        }),
        CompiledFilter::Contains(f, v) => {
            if let Some(dv) = doc.data.get(f) {
                match (dv, resolve_if_variable(v, vars)) {
                    (Value::String(s), ast::Value::String(sub)) => s.contains(sub),
                    (Value::Array(arr), _) => {
                        if let Ok(bv) = aql_value_to_db_value(v, vars) {
                            return arr.contains(&bv);
                        }
                        false
                    }
                    _ => false,
                }
            } else {
                false
            }
        }
        CompiledFilter::StartsWith(f, v) => {
            if let (Some(Value::String(s)), ast::Value::String(pre)) =
                (doc.data.get(f), resolve_if_variable(v, vars))
            {
                s.starts_with(pre)
            } else {
                false
            }
        }
        CompiledFilter::EndsWith(f, v) => {
            if let (Some(Value::String(s)), ast::Value::String(suf)) =
                (doc.data.get(f), resolve_if_variable(v, vars))
            {
                s.ends_with(suf)
            } else {
                false
            }
        }
        CompiledFilter::Matches(f, re) => {
            if let Some(Value::String(s)) = doc.data.get(f) {
                re.is_match(s)
            } else {
                false
            }
        }
        CompiledFilter::IsNull(f) => doc.data.get(f).map_or(true, |v| matches!(v, Value::Null)),
        CompiledFilter::IsNotNull(f) => {
            doc.data.get(f).map_or(false, |v| !matches!(v, Value::Null))
        }
        CompiledFilter::And(fs) => fs.iter().all(|f| matches_filter(doc, f, vars)),
        CompiledFilter::Or(fs) => fs.iter().any(|f| matches_filter(doc, f, vars)),
        CompiledFilter::Not(f) => !matches_filter(doc, f, vars),
    }
}

fn values_equal(dv: &Value, av: &ast::Value, vars: &HashMap<String, ast::Value>) -> bool {
    if let Ok(bv) = aql_value_to_db_value(av, vars) {
        return dv == &bv;
    }
    false
}

fn resolve_if_variable<'a>(
    v: &'a ast::Value,
    vars: &'a HashMap<String, ast::Value>,
) -> &'a ast::Value {
    if let ast::Value::Variable(n) = v {
        vars.get(n).unwrap_or(v)
    } else {
        v
    }
}

pub fn apply_projection(mut doc: Document, fields: &[ast::Field]) -> Document {
    if fields.is_empty() {
        return doc;
    }
    let mut proj = HashMap::new();

    // Always preserve the 'id' field as it's a core document property
    // proj.insert("id".to_string(), Value::String(doc.id.clone()));
    // Actually, in Aurora, 'id' is a first-class field on the Document struct itself,
    // but users often expect it in the data map too during projection.

    for f in fields {
        if f.name == "id" {
            proj.insert(
                f.alias.as_ref().unwrap_or(&f.name).clone(),
                Value::String(doc.id.clone()),
            );
        } else if let Some(v) = doc.data.get(&f.name) {
            proj.insert(f.alias.as_ref().unwrap_or(&f.name).clone(), v.clone());
        }
    }
    doc.data = proj;
    doc
}

async fn apply_projection_with_lookups(
    _db: &Aurora,
    doc: Document,
    _fields: &[ast::Selection],
    _vars: &HashMap<String, ast::Value>,
    _frags: &HashMap<String, FragmentDef>,
    _coll: &str,
) -> Result<Document> {
    Ok(doc)
}

pub fn aql_value_to_db_value(v: &ast::Value, vars: &HashMap<String, ast::Value>) -> Result<Value> {
    let resolved = resolve_if_variable(v, vars);
    match resolved {
        ast::Value::Int(i) => Ok(Value::Int(*i)),
        ast::Value::Float(f) => Ok(Value::Float(*f)),
        ast::Value::Boolean(b) => Ok(Value::Bool(*b)),
        ast::Value::String(s) => Ok(Value::String(s.clone())),
        ast::Value::Enum(s) => Ok(Value::String(s.clone())),
        ast::Value::Null => Ok(Value::Null),
        ast::Value::Variable(name) => Err(AqlError::new(
            ErrorCode::UndefinedVariable,
            format!("Variable '{}' not found", name),
        )),
        ast::Value::Array(arr) => {
            let mut vals = Vec::with_capacity(arr.len());
            for v in arr {
                vals.push(aql_value_to_db_value(v, vars)?);
            }
            Ok(Value::Array(vals))
        }
        ast::Value::Object(obj) => {
            let mut map = HashMap::with_capacity(obj.len());
            for (k, v) in obj {
                map.insert(k.clone(), aql_value_to_db_value(v, vars)?);
            }
            Ok(Value::Object(map))
        }
    }
}

fn aql_value_to_hashmap(
    v: &ast::Value,
    vars: &HashMap<String, ast::Value>,
) -> Result<HashMap<String, Value>> {
    if let ast::Value::Object(m) = resolve_if_variable(v, vars) {
        let mut res = HashMap::new();
        for (k, v) in m {
            res.insert(k.clone(), aql_value_to_db_value(v, vars)?);
        }
        Ok(res)
    } else {
        Err(AqlError::new(
            ErrorCode::QueryError,
            "Data must be object".to_string(),
        ))
    }
}

fn aurora_value_to_json_value(v: &Value) -> JsonValue {
    match v {
        Value::Null => JsonValue::Null,
        Value::String(s) => JsonValue::String(s.clone()),
        Value::Int(i) => JsonValue::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Array(arr) => JsonValue::Array(arr.iter().map(aurora_value_to_json_value).collect()),
        Value::Object(m) => {
            let mut jm = serde_json::Map::new();
            for (k, v) in m {
                jm.insert(k.clone(), aurora_value_to_json_value(v));
            }
            JsonValue::Object(jm)
        }
        Value::Uuid(u) => JsonValue::String(u.to_string()),
        Value::DateTime(dt) => JsonValue::String(dt.to_rfc3339()),
    }
}

/// Find an equality filter on an indexed field to allow O(1) lookup short-circuit
fn find_indexed_equality_filter(
    filter: &AqlFilter,
    db: &Aurora,
    collection: &str,
) -> Option<(String, ast::Value)> {
    match filter {
        AqlFilter::Eq(field, val) => {
            if field == "id" || db.has_index(collection, field) {
                Some((field.clone(), val.clone()))
            } else {
                None
            }
        }
        AqlFilter::And(filters) => {
            for f in filters {
                if let Some(res) = find_indexed_equality_filter(f, db, collection) {
                    return Some(res);
                }
            }
            None
        }
        _ => None,
    }
}

pub fn value_to_filter(v: &ast::Value) -> Result<AqlFilter> {
    if let ast::Value::Object(m) = v {
        let mut fs = Vec::new();
        for (k, val) in m {
            match k.as_str() {
                "or" => {
                    if let ast::Value::Array(arr) = val {
                        let mut sub = Vec::new();
                        for item in arr {
                            sub.push(value_to_filter(item)?);
                        }
                        return Ok(AqlFilter::Or(sub));
                    }
                }
                "and" => {
                    if let ast::Value::Array(arr) = val {
                        let mut sub = Vec::new();
                        for item in arr {
                            sub.push(value_to_filter(item)?);
                        }
                        return Ok(AqlFilter::And(sub));
                    }
                }
                "not" => {
                    return Ok(AqlFilter::Not(Box::new(value_to_filter(val)?)));
                }
                field => {
                    if let ast::Value::Object(ops) = val {
                        for (op, ov) in ops {
                            match op.as_str() {
                                "eq" => fs.push(AqlFilter::Eq(field.to_string(), ov.clone())),
                                "ne" => fs.push(AqlFilter::Ne(field.to_string(), ov.clone())),
                                "gt" => fs.push(AqlFilter::Gt(field.to_string(), ov.clone())),
                                "gte" => fs.push(AqlFilter::Gte(field.to_string(), ov.clone())),
                                "lt" => fs.push(AqlFilter::Lt(field.to_string(), ov.clone())),
                                "lte" => fs.push(AqlFilter::Lte(field.to_string(), ov.clone())),
                                "in" => fs.push(AqlFilter::In(field.to_string(), ov.clone())),
                                "notin" => fs.push(AqlFilter::NotIn(field.to_string(), ov.clone())),
                                "contains" => {
                                    fs.push(AqlFilter::Contains(field.to_string(), ov.clone()))
                                }
                                "startsWith" => {
                                    fs.push(AqlFilter::StartsWith(field.to_string(), ov.clone()))
                                }
                                "endsWith" => {
                                    fs.push(AqlFilter::EndsWith(field.to_string(), ov.clone()))
                                }
                                "matches" => {
                                    fs.push(AqlFilter::Matches(field.to_string(), ov.clone()))
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
        if fs.is_empty() {
            Ok(AqlFilter::And(vec![]))
        } else if fs.len() == 1 {
            Ok(fs.remove(0))
        } else {
            Ok(AqlFilter::And(fs))
        }
    } else {
        Err(AqlError::new(
            ErrorCode::QueryError,
            "Filter must be object".to_string(),
        ))
    }
}

fn resolve_value(
    v: &ast::Value,
    vars: &HashMap<String, ast::Value>,
    _ctx: &ExecutionContext,
) -> ast::Value {
    match v {
        ast::Value::Variable(n) => vars.get(n).cloned().unwrap_or(ast::Value::Null),
        _ => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Aurora, AuroraConfig, DurabilityMode, FieldType};
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_executor_integration() {
        let td = TempDir::new().unwrap();
        let db = Aurora::with_config(AuroraConfig {
            db_path: td.path().join("test.db"),
            enable_write_buffering: false,
            durability_mode: DurabilityMode::Strict,
            ..Default::default()
        })
        .await
        .unwrap();
        db.new_collection(
            "users",
            vec![(
                "name",
                crate::types::FieldDefinition {
                    field_type: FieldType::SCALAR_STRING,
                    unique: false,
                    indexed: true,
                    nullable: true,
                },
            )],
        )
        .await
        .unwrap();
        let _ = execute(
            &db,
            r#"mutation { insertInto(collection: "users", data: { name: "Alice" }) { id name } }"#,
            ExecutionOptions::new(),
        )
        .await
        .unwrap();
        let res = execute(&db, "query { users { name } }", ExecutionOptions::new())
            .await
            .unwrap();
        if let ExecutionResult::Query(q) = res {
            assert_eq!(q.documents.len(), 1);
        } else {
            panic!("Expected query");
        }
    }
}
