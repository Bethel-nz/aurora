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

// chrono used for downsample timestamp bucketing
#[allow(unused_imports)]
use chrono::TimeZone as _;

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
        variables: &HashMap<String, ast::Value>,
    ) -> Result<Vec<Self>> {
        let root_fields = collect_fields(&query.selection_set, fragments, variables, None)?;

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
                variables,
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

        let plans = QueryPlan::from_query(query, &fragments, &vars)?;
        if plans.len() == 1 {
            let plan = Arc::new(plans[0].clone());
            plan.validate(&vars)?;
            db.plan_cache.insert(query_key, Arc::clone(&plan));

            return execute_plan(db, &plan, &vars, &options).await;
        }
    }

    // FALLBACK: Normal execution for complex documents (but WITH our prepared vars)
    // Merge declared variable defaults for any var not explicitly provided.
    let mut vars = vars;
    for op in &doc.operations {
        if let Operation::Query(q) = op {
            for var_def in &q.variable_definitions {
                if !vars.contains_key(&var_def.name) {
                    if let Some(default) = &var_def.default_value {
                        vars.insert(var_def.name.clone(), default.clone());
                    }
                }
            }
        }
    }

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

        // Scan-limit is only valid when there is no cursor and no ordering —
        // orderings require a full scan to find the correct top-N.
        let scan_limit = if plan.after.is_some() || !plan.orderings.is_empty() {
            None
        } else {
            plan.limit.map(|l| {
                let base = if plan.is_connection { l + 1 } else { l };
                base + plan.offset
            })
        };
        db.scan_and_filter(collection_name, filter_fn, scan_limit)?
    };

    // 2. Sort (must happen before cursor drain and offset)
    if !plan.orderings.is_empty() {
        apply_ordering(&mut docs, &plan.orderings);
    }

    // 2a. Cursor pagination: skip past the `after` cursor
    if let Some(ref cursor) = plan.after {
        if let Some(pos) = docs.iter().position(|d| &d.id == cursor) {
            docs.drain(0..=pos);
        }
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

    // 3a. Apply limit for flat list (after ordering so we get correct top-N)
    if let Some(l) = effective_limit {
        docs.truncate(l);
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
            let mut projected = Vec::with_capacity(docs.len());
            for d in docs {
                let (proj_doc, _) =
                    apply_projection_with_lookups(db, d, &plan.projection, variables).await?;
                projected.push(proj_doc);
            }
            docs = projected;
        }
    }

    Ok(ExecutionResult::Query(QueryResult {
        collection: collection_name.clone(),
        documents: docs,
        total_count: None,
        deferred_fields: vec![],
        explain: None,
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
    /// Fields marked @defer — omitted from documents, listed here
    pub deferred_fields: Vec<String>,
    /// Explain metadata when @explain is present on the query
    pub explain: Option<ExplainResult>,
}

/// Execution plan metadata returned by @explain
#[derive(Debug, Clone, Default)]
pub struct ExplainResult {
    pub collection: String,
    pub docs_scanned: usize,
    pub index_used: bool,
    pub elapsed_ms: u128,
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
        Operation::Handler(handler) => execute_handler_registration(db, handler, options).await,
        _ => Ok(ExecutionResult::Query(QueryResult {
            collection: String::new(),
            documents: vec![],
            total_count: None,
            deferred_fields: vec![],
            explain: None,
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
    let has_explain = query.directives.iter().any(|d| d.name == "explain");
    let root_fields = collect_fields(&query.selection_set, fragments, vars, None)?;
    let mut results = Vec::new();
    for field in &root_fields {
        let sub_fields = collect_fields(&field.selection_set, fragments, vars, Some(&field.name))?;
        let start = std::time::Instant::now();
        let mut result =
            execute_collection_query(db, field, &sub_fields, vars, options, fragments).await?;
        if has_explain {
            let elapsed_ms = start.elapsed().as_millis();
            let index_used =
                field.arguments.iter().any(|a| a.name == "where") && !result.documents.is_empty();
            result.explain = Some(ExplainResult {
                collection: result.collection.clone(),
                docs_scanned: result.documents.len(),
                index_used,
                elapsed_ms,
            });
        }
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

    // ── Search args ────────────────────────────────────────────────────────
    // `search: { query: "...", fields: [...], fuzzy: true }` overrides normal scan.
    if let Some(search_arg) = field.arguments.iter().find(|a| a.name == "search") {
        return execute_search_query(
            db,
            collection_name,
            search_arg,
            sub_fields,
            field,
            variables,
            options,
        )
        .await;
    }

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

    let orderings = extract_order_by(&field.arguments);

    let mut docs = if let Some(docs) = indexed_docs {
        docs
    } else {
        //only valid when there is no cursor (after) and no
        // ordering.  With ordering we must see ALL docs before we can pick the top-N;
        // with a cursor we don't know how far into the result set the cursor lies.
        let scan_limit = if after.is_some() || !orderings.is_empty() {
            None
        } else {
            limit.or(first).map(|l| {
                let base = if is_connection { l + 1 } else { l };
                base + offset
            })
        };
        db.scan_and_filter(collection_name, filter_fn, scan_limit)?
    };

    // Validation args
    // `validate: { fieldName: { format: "email" } }` filters out non-conforming docs
    if let Some(validate_arg) = field.arguments.iter().find(|a| a.name == "validate") {
        docs.retain(|doc| doc_passes_validate_arg(doc, validate_arg));
    }

    // Sort before any pagination so offset/cursor operate on ordered results.
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

    // Apply limit after offset
    if let Some(l) = limit.or(first) {
        docs.truncate(l);
    }

    // Projections & Lookups
    let has_lookups = sub_fields.iter().any(|f| {
        f.arguments
            .iter()
            .any(|a| a.name == "collection" || a.name == "localField")
    });

    let mut deferred_fields = Vec::new();

    if options.apply_projections && !sub_fields.is_empty() {
        if has_lookups {
            let mut projected = Vec::with_capacity(docs.len());
            for d in docs {
                let (proj_doc, deferred) =
                    apply_projection_with_lookups(db, d, sub_fields, variables).await?;
                projected.push(proj_doc);
                if deferred_fields.is_empty() {
                    deferred_fields = deferred;
                }
            }
            docs = projected;
        } else {
            let mut all_deferred = Vec::new();
            docs = docs
                .into_iter()
                .map(|d| {
                    let (proj, deferred) = apply_projection_and_defer(d, sub_fields);
                    if all_deferred.is_empty() && !deferred.is_empty() {
                        all_deferred = deferred;
                    }
                    proj
                })
                .collect();
            deferred_fields = all_deferred;
        }
    }

    // ── Window functions ───────────────────────────────────────────────────
    for sf in sub_fields {
        if sf.name == "windowFunc" {
            let alias = sf.alias.as_ref().unwrap_or(&sf.name).clone();
            let wfield = arg_string(&sf.arguments, "field").unwrap_or_default();
            let func = arg_string(&sf.arguments, "function").unwrap_or_else(|| "avg".to_string());
            let wsize = arg_i64(&sf.arguments, "windowSize").unwrap_or(3) as usize;
            apply_window_function(&mut docs, &alias, &wfield, &func, wsize);
        }
    }

    // ── Downsample ─────────────────────────────────────────────────────────
    if let Some(ds_field) = sub_fields.iter().find(|f| f.name == "downsample") {
        let interval =
            arg_string(&ds_field.arguments, "interval").unwrap_or_else(|| "1h".to_string());
        let aggregation =
            arg_string(&ds_field.arguments, "aggregation").unwrap_or_else(|| "avg".to_string());
        let ds_sub: Vec<String> =
            collect_fields(&ds_field.selection_set, fragments, variables, None)
                .unwrap_or_default()
                .iter()
                .map(|f| f.name.clone())
                .collect();
        docs = apply_downsample(docs, &interval, &aggregation, &ds_sub);
    }

    Ok(QueryResult {
        collection: collection_name.clone(),
        documents: docs,
        total_count: None,
        deferred_fields,
        explain: None,
    })
}

/// Execute a search query using SearchBuilder
async fn execute_search_query(
    db: &Aurora,
    collection: &str,
    search_arg: &ast::Argument,
    sub_fields: &[ast::Field],
    field: &ast::Field,
    variables: &HashMap<String, ast::Value>,
    options: &ExecutionOptions,
) -> Result<QueryResult> {
    // Deeply resolve variable references inside the search argument (e.g. query: $term)
    let resolved_search_val = resolve_ast_deep(&search_arg.value, variables);
    let (query_str, search_fields, fuzzy) = extract_search_params(&resolved_search_val);
    let (limit, _) = extract_pagination(&field.arguments);

    let mut builder = db.search(collection).query(&query_str);
    if fuzzy {
        builder = builder.fuzzy(1);
    }
    if let Some(l) = limit {
        builder = builder.limit(l);
    }

    let mut docs = builder
        .collect_with_fields(if search_fields.is_empty() {
            None
        } else {
            Some(&search_fields)
        })
        .await?;

    if options.apply_projections && !sub_fields.is_empty() {
        docs = docs
            .into_iter()
            .map(|d| {
                let (proj, _) = apply_projection_and_defer(d, sub_fields);
                proj
            })
            .collect();
    }

    Ok(QueryResult {
        collection: collection.to_string(),
        documents: docs,
        total_count: None,
        deferred_fields: vec![],
        explain: None,
    })
}

fn extract_search_params(v: &ast::Value) -> (String, Vec<String>, bool) {
    let mut query = String::new();
    let mut fields = Vec::new();
    let mut fuzzy = false;
    if let ast::Value::Object(m) = v {
        if let Some(ast::Value::String(q)) = m.get("query") {
            query = q.clone();
        }
        if let Some(ast::Value::Array(arr)) = m.get("fields") {
            for item in arr {
                if let ast::Value::String(s) = item {
                    fields.push(s.clone());
                }
            }
        }
        if let Some(ast::Value::Boolean(b)) = m.get("fuzzy") {
            fuzzy = *b;
        }
    }
    (query, fields, fuzzy)
}

fn doc_passes_validate_arg(doc: &Document, validate_arg: &ast::Argument) -> bool {
    if let ast::Value::Object(rules) = &validate_arg.value {
        for (field_name, constraints_val) in rules {
            if let ast::Value::Object(constraints) = constraints_val {
                if let Some(field_val) = doc.data.get(field_name) {
                    for (constraint_name, constraint_val) in constraints {
                        if !check_inline_constraint(field_val, constraint_name, constraint_val) {
                            return false;
                        }
                    }
                }
            }
        }
    }
    true
}

fn check_inline_constraint(value: &Value, constraint: &str, constraint_val: &ast::Value) -> bool {
    match constraint {
        "format" => {
            if let (Value::String(s), ast::Value::String(fmt)) = (value, constraint_val) {
                return match fmt.as_str() {
                    "email" => {
                        s.contains('@')
                            && s.split('@')
                                .nth(1)
                                .map(|d| d.contains('.'))
                                .unwrap_or(false)
                    }
                    "url" => s.starts_with("http://") || s.starts_with("https://"),
                    "uuid" => uuid::Uuid::parse_str(s).is_ok(),
                    _ => true,
                };
            }
            true
        }
        "min" => {
            let n = match value {
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                _ => return true,
            };
            let min = match constraint_val {
                ast::Value::Float(f) => *f,
                ast::Value::Int(i) => *i as f64,
                _ => return true,
            };
            n >= min
        }
        "max" => {
            let n = match value {
                Value::Int(i) => *i as f64,
                Value::Float(f) => *f,
                _ => return true,
            };
            let max = match constraint_val {
                ast::Value::Float(f) => *f,
                ast::Value::Int(i) => *i as f64,
                _ => return true,
            };
            n <= max
        }
        "minLength" => {
            if let (Value::String(s), ast::Value::Int(n)) = (value, constraint_val) {
                return s.len() >= *n as usize;
            }
            true
        }
        "maxLength" => {
            if let (Value::String(s), ast::Value::Int(n)) = (value, constraint_val) {
                return s.len() <= *n as usize;
            }
            true
        }
        "pattern" => {
            if let (Value::String(s), ast::Value::String(pat)) = (value, constraint_val) {
                if let Ok(re) = regex::Regex::new(pat) {
                    return re.is_match(s);
                }
            }
            true
        }
        _ => true,
    }
}

fn arg_string(args: &[ast::Argument], name: &str) -> Option<String> {
    args.iter().find(|a| a.name == name).and_then(|a| {
        if let ast::Value::String(s) = &a.value {
            Some(s.clone())
        } else {
            None
        }
    })
}

fn arg_i64(args: &[ast::Argument], name: &str) -> Option<i64> {
    args.iter().find(|a| a.name == name).and_then(|a| {
        if let ast::Value::Int(i) = &a.value {
            Some(*i)
        } else {
            None
        }
    })
}

/// Apply projection and collect @defer'd field names separately
fn apply_projection_and_defer(mut doc: Document, fields: &[ast::Field]) -> (Document, Vec<String>) {
    if fields.is_empty() {
        return (doc, vec![]);
    }
    let mut proj = HashMap::new();
    let mut deferred = Vec::new();

    for f in fields {
        // @defer directive: omit from response, record field name
        if f.directives.iter().any(|d| d.name == "defer") {
            deferred.push(f.alias.as_ref().unwrap_or(&f.name).clone());
            continue;
        }
        // Computed field: name starts with __compute__ sentinel
        if f.name == "__compute__" {
            let alias = f.alias.as_deref().unwrap_or("computed");
            if let Some(expr) = f.arguments.iter().find(|a| a.name == "expr") {
                if let ast::Value::String(template) = &expr.value {
                    let result = eval_template(template, &doc.data);
                    proj.insert(alias.to_string(), Value::String(result));
                }
            }
            continue;
        }
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
    (doc, deferred)
}

/// Simple `$fieldName` template evaluator
fn eval_template(template: &str, data: &HashMap<String, Value>) -> String {
    let mut result = template.to_string();
    for (k, v) in data {
        let placeholder = format!("${}", k);
        if result.contains(&placeholder) {
            result = result.replace(&placeholder, &v.to_string());
        }
    }
    result
}

/// Apply window function (rollingAvg, rollingSum, rollingMin, rollingMax) on the docs Vec
fn apply_window_function(
    docs: &mut Vec<Document>,
    alias: &str,
    field: &str,
    function: &str,
    window: usize,
) {
    if docs.is_empty() || window == 0 {
        return;
    }
    let values: Vec<Option<f64>> = docs
        .iter()
        .map(|d| match d.data.get(field) {
            Some(Value::Int(i)) => Some(*i as f64),
            Some(Value::Float(f)) => Some(*f),
            _ => None,
        })
        .collect();

    for (i, doc) in docs.iter_mut().enumerate() {
        let start = if i + 1 >= window { i + 1 - window } else { 0 };
        let window_vals: Vec<f64> = values[start..=i].iter().filter_map(|v| *v).collect();
        if window_vals.is_empty() {
            continue;
        }
        let result = match function {
            "rollingAvg" | "avg" => window_vals.iter().sum::<f64>() / window_vals.len() as f64,
            "rollingSum" | "sum" => window_vals.iter().sum::<f64>(),
            "rollingMin" | "min" => window_vals.iter().cloned().fold(f64::INFINITY, f64::min),
            "rollingMax" | "max" => window_vals
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max),
            _ => window_vals.iter().sum::<f64>() / window_vals.len() as f64,
        };
        doc.data.insert(alias.to_string(), Value::Float(result));
    }
}

/// Downsample time-series: bucket docs by interval, aggregate value fields
fn apply_downsample(
    docs: Vec<Document>,
    interval: &str,
    aggregation: &str,
    value_fields: &[String],
) -> Vec<Document> {
    let interval_secs: i64 = parse_interval(interval);
    if interval_secs <= 0 {
        return docs;
    }

    // Group by bucket
    let mut buckets: std::collections::BTreeMap<i64, Vec<Document>> =
        std::collections::BTreeMap::new();
    let mut leftover = Vec::new();

    for doc in docs {
        // Try common timestamp field names
        let ts = ["timestamp", "ts", "created_at", "time"]
            .iter()
            .find_map(|&k| doc.data.get(k))
            .and_then(|v| match v {
                Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
                    .ok()
                    .map(|dt| dt.timestamp()),
                Value::Int(i) => Some(*i),
                _ => None,
            });

        if let Some(t) = ts {
            let bucket = (t / interval_secs) * interval_secs;
            buckets.entry(bucket).or_default().push(doc);
        } else {
            leftover.push(doc);
        }
    }

    let mut result = Vec::new();
    for (bucket_ts, group) in buckets {
        let mut data = HashMap::new();
        data.insert(
            "timestamp".to_string(),
            Value::String(
                chrono::DateTime::from_timestamp(bucket_ts, 0)
                    .map(|dt: chrono::DateTime<chrono::Utc>| dt.to_rfc3339())
                    .unwrap_or_default(),
            ),
        );
        data.insert("count".to_string(), Value::Int(group.len() as i64));

        for field in value_fields {
            if field == "timestamp" || field == "count" {
                continue;
            }
            let nums: Vec<f64> = group
                .iter()
                .filter_map(|d| match d.data.get(field) {
                    Some(Value::Int(i)) => Some(*i as f64),
                    Some(Value::Float(f)) => Some(*f),
                    _ => None,
                })
                .collect();
            if nums.is_empty() {
                continue;
            }
            let agg = match aggregation {
                "sum" => nums.iter().sum::<f64>(),
                "min" => nums.iter().cloned().fold(f64::INFINITY, f64::min),
                "max" => nums.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                "count" => nums.len() as f64,
                _ => nums.iter().sum::<f64>() / nums.len() as f64, // avg default
            };
            data.insert(field.clone(), Value::Float(agg));
        }

        result.push(Document {
            id: bucket_ts.to_string(),
            data,
        });
    }

    result.extend(leftover);
    result
}

fn parse_interval(s: &str) -> i64 {
    let s = s.trim();
    if s.ends_with('s') {
        s[..s.len() - 1].parse().unwrap_or(0)
    } else if s.ends_with('m') {
        s[..s.len() - 1].parse::<i64>().unwrap_or(0) * 60
    } else if s.ends_with('h') {
        s[..s.len() - 1].parse::<i64>().unwrap_or(0) * 3600
    } else if s.ends_with('d') {
        s[..s.len() - 1].parse::<i64>().unwrap_or(0) * 86400
    } else {
        s.parse().unwrap_or(3600)
    }
}

/// Register an event handler. Spawns a background task that fires a mutation
/// each time the trigger event is received on the relevant collection.
async fn execute_handler_registration(
    db: &Aurora,
    handler: &ast::HandlerDef,
    _options: &ExecutionOptions,
) -> Result<ExecutionResult> {
    use crate::pubsub::events::ChangeType;

    let collection = match &handler.trigger {
        ast::HandlerTrigger::Insert { collection }
        | ast::HandlerTrigger::Update { collection }
        | ast::HandlerTrigger::Delete { collection } => {
            collection.as_deref().unwrap_or("*").to_string()
        }
        _ => "*".to_string(),
    };

    let trigger_type = match &handler.trigger {
        ast::HandlerTrigger::Insert { .. } => Some(ChangeType::Insert),
        ast::HandlerTrigger::Update { .. } => Some(ChangeType::Update),
        ast::HandlerTrigger::Delete { .. } => Some(ChangeType::Delete),
        _ => None,
    };

    let mut listener = if collection == "*" {
        db.pubsub.listen_all()
    } else {
        db.pubsub.listen(collection.clone())
    };

    let db_clone = db.clone();
    let action = handler.action.clone();
    let handler_name = handler.name.clone();

    tokio::spawn(async move {
        loop {
            match listener.recv().await {
                Ok(event) => {
                    // Check event type matches trigger
                    let matches = trigger_type
                        .as_ref()
                        .map(|t| &event.change_type == t)
                        .unwrap_or(true);
                    if !matches {
                        continue;
                    }

                    // Build context from the triggering event.
                    // Always set $_id from event.id — for deletes, event.document is None
                    // but the document ID is still in event.id.
                    let mut vars = HashMap::new();
                    vars.insert("_id".to_string(), ast::Value::String(event.id.clone()));
                    if let Some(doc) = &event.document {
                        // Overlay field variables from the document data (insert/update)
                        for (k, v) in &doc.data {
                            vars.insert(format!("_{}", k), db_value_to_ast_value(v));
                        }
                    }

                    let _ = execute_mutation_op(
                        &db_clone,
                        &action,
                        &vars,
                        &HashMap::new(),
                        &ExecutionOptions::default(),
                        &HashMap::new(),
                    )
                    .await;
                }
                Err(_) => {
                    eprintln!("[handler:{}] channel closed, stopping", handler_name);
                    break;
                }
            }
        }
    });

    eprintln!(
        "[handler] '{}' registered on '{}'",
        handler.name, collection
    );

    let mut data = HashMap::new();
    data.insert("name".to_string(), Value::String(handler.name.clone()));
    data.insert("collection".to_string(), Value::String(collection));
    data.insert(
        "status".to_string(),
        Value::String("registered".to_string()),
    );

    Ok(ExecutionResult::Query(QueryResult {
        collection: "__handler".to_string(),
        documents: vec![Document {
            id: handler.name.clone(),
            data,
        }],
        total_count: Some(1),
        deferred_fields: vec![],
        explain: None,
    }))
}

fn db_value_to_ast_value(v: &Value) -> ast::Value {
    match v {
        Value::Null => ast::Value::Null,
        Value::Bool(b) => ast::Value::Boolean(*b),
        Value::Int(i) => ast::Value::Int(*i),
        Value::Float(f) => ast::Value::Float(*f),
        Value::String(s) => ast::Value::String(s.clone()),
        Value::Uuid(u) => ast::Value::String(u.to_string()),
        Value::DateTime(dt) => ast::Value::String(dt.to_rfc3339()),
        Value::Array(arr) => ast::Value::Array(arr.iter().map(db_value_to_ast_value).collect()),
        Value::Object(m) => ast::Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), db_value_to_ast_value(v)))
                .collect(),
        ),
    }
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

    // If there's already an active transaction in scope, run ops directly inside
    // it — the caller owns commit/rollback. Creating a nested transaction would
    // shadow the outer ACTIVE_TRANSACTION_ID and prevent the outer rollback from
    // undoing these writes.
    let already_in_tx = ACTIVE_TRANSACTION_ID
        .try_with(|id| *id)
        .ok()
        .and_then(|id| db.transaction_manager.active_transactions.get(&id))
        .is_some();

    if already_in_tx {
        let mut results = Vec::new();
        let mut context = HashMap::new();
        for mut_op in &mutation.operations {
            let res = execute_mutation_op(db, mut_op, vars, &context, options, fragments).await?;
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
        return if results.len() == 1 {
            Ok(ExecutionResult::Mutation(results.remove(0)))
        } else {
            Ok(ExecutionResult::Batch(
                results.into_iter().map(ExecutionResult::Mutation).collect(),
            ))
        };
    }

    // No active transaction — wrap the mutation block in one so a failure in any
    // operation rolls back all previous operations in the same block.
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
                        let applied = apply_field_modifier(new_data.get(k), v);
                        new_data.insert(k.clone(), applied);
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
            MutationOp::InsertMany { collection, data } => {
                let mut affected = 0;
                let mut returned = Vec::new();
                for item in data {
                    let resolved = resolve_value(item, variables, context);
                    let doc = db
                        .aql_insert(collection, aql_value_to_hashmap(&resolved, variables)?)
                        .await?;
                    affected += 1;
                    if !mut_op.selection_set.is_empty() && options.apply_projections {
                        let fields = collect_fields(
                            &mut_op.selection_set,
                            fragments,
                            variables,
                            Some(collection),
                        )
                        .unwrap_or_default();
                        returned.push(apply_projection(doc, &fields));
                    } else {
                        returned.push(doc);
                    }
                }
                Ok(MutationResult {
                    operation: "insertMany".to_string(),
                    collection: collection.clone(),
                    affected_count: affected,
                    returned_documents: returned,
                })
            }
            MutationOp::Upsert {
                collection,
                filter,
                data,
            } => {
                let update_data =
                    aql_value_to_hashmap(&resolve_value(data, variables, context), variables)?;
                let cf = if let Some(f) = filter {
                    Some(compile_filter(f)?)
                } else {
                    None
                };
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
                    Some(1),
                )?;
                let doc = if let Some(existing) = matches.into_iter().next() {
                    db.aql_update_document(collection, &existing.id, update_data)
                        .await?
                } else {
                    db.aql_insert(collection, update_data).await?
                };
                Ok(MutationResult {
                    operation: "upsert".to_string(),
                    collection: collection.clone(),
                    affected_count: 1,
                    returned_documents: vec![doc],
                })
            }
            MutationOp::Transaction { operations } => {
                let mut all_returned = Vec::new();
                let mut total_affected = 0;
                for op in operations {
                    let res =
                        execute_mutation_op(db, op, variables, context, options, fragments).await?;
                    total_affected += res.affected_count;
                    all_returned.extend(res.returned_documents);
                }
                Ok(MutationResult {
                    operation: "transaction".to_string(),
                    collection: String::new(),
                    affected_count: total_affected,
                    returned_documents: all_returned,
                })
            }
            MutationOp::EnqueueJobs {
                job_type,
                payloads,
                priority,
                max_retries,
            } => {
                let workers = db.workers.as_ref().ok_or_else(|| {
                    AqlError::new(
                        ErrorCode::QueryError,
                        "Worker system not initialised".to_string(),
                    )
                })?;
                let job_priority = match priority {
                    ast::JobPriority::Low => crate::workers::JobPriority::Low,
                    ast::JobPriority::Normal => crate::workers::JobPriority::Normal,
                    ast::JobPriority::High => crate::workers::JobPriority::High,
                    ast::JobPriority::Critical => crate::workers::JobPriority::Critical,
                };
                let mut returned = Vec::new();
                for payload in payloads {
                    let resolved = resolve_value(payload, variables, context);
                    let json_payload: std::collections::HashMap<String, serde_json::Value> =
                        if let ast::Value::Object(map) = &resolved {
                            map.iter()
                                .map(|(k, v)| (k.clone(), aql_value_to_json(v)))
                                .collect()
                        } else {
                            std::collections::HashMap::new()
                        };
                    let mut job =
                        crate::workers::Job::new(job_type.clone()).with_priority(job_priority);
                    for (k, v) in json_payload {
                        job = job.add_field(k, v);
                    }
                    if let Some(retries) = max_retries {
                        job = job.with_max_retries(*retries);
                    }
                    let job_id = workers.enqueue(job).await?;
                    let mut doc = crate::types::Document::new();
                    doc.id = job_id.clone();
                    doc.data.insert("job_id".to_string(), Value::String(job_id));
                    doc.data
                        .insert("job_type".to_string(), Value::String(job_type.clone()));
                    doc.data
                        .insert("status".to_string(), Value::String("pending".to_string()));
                    returned.push(doc);
                }
                let count = returned.len();
                Ok(MutationResult {
                    operation: "enqueueJobs".to_string(),
                    collection: "__jobs".to_string(),
                    affected_count: count,
                    returned_documents: returned,
                })
            }
            MutationOp::Import { collection, data } => {
                let mut affected = 0;
                let mut returned = Vec::new();
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
                for item in data {
                    let resolved = resolve_value(item, variables, context);
                    let map = aql_value_to_hashmap(&resolved, variables)?;
                    let doc = db.aql_insert(collection, map).await?;
                    affected += 1;
                    if let Some(ref f) = fields {
                        returned.push(apply_projection(doc, f));
                    }
                }
                Ok(MutationResult {
                    operation: "import".to_string(),
                    collection: collection.clone(),
                    affected_count: affected,
                    returned_documents: returned,
                })
            }
            MutationOp::Export {
                collection,
                format: _,
            } => {
                let docs = db.scan_and_filter(collection, |_| true, None)?;
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
                let returned: Vec<Document> = if let Some(ref f) = fields {
                    docs.into_iter().map(|d| apply_projection(d, f)).collect()
                } else {
                    docs
                };
                let count = returned.len();
                Ok(MutationResult {
                    operation: "export".to_string(),
                    collection: collection.clone(),
                    affected_count: count,
                    returned_documents: returned,
                })
            }
            MutationOp::EnqueueJob {
                job_type,
                payload,
                priority,
                scheduled_at,
                max_retries,
            } => {
                let workers = db.workers.as_ref().ok_or_else(|| {
                    AqlError::new(
                        ErrorCode::QueryError,
                        "Worker system not initialised".to_string(),
                    )
                })?;

                // Convert AQL priority to Job priority
                let job_priority = match priority {
                    ast::JobPriority::Low => crate::workers::JobPriority::Low,
                    ast::JobPriority::Normal => crate::workers::JobPriority::Normal,
                    ast::JobPriority::High => crate::workers::JobPriority::High,
                    ast::JobPriority::Critical => crate::workers::JobPriority::Critical,
                };

                // Convert AQL Value payload to serde_json payload
                let resolved = resolve_value(payload, variables, context);
                let json_payload: std::collections::HashMap<String, serde_json::Value> =
                    if let ast::Value::Object(map) = &resolved {
                        map.iter()
                            .map(|(k, v)| (k.clone(), aql_value_to_json(v)))
                            .collect()
                    } else {
                        std::collections::HashMap::new()
                    };

                let mut job =
                    crate::workers::Job::new(job_type.clone()).with_priority(job_priority);

                for (k, v) in json_payload {
                    job = job.add_field(k, v);
                }

                if let Some(retries) = max_retries {
                    job = job.with_max_retries(*retries);
                }

                if let Some(scheduled) = scheduled_at {
                    if let Ok(dt) = scheduled.parse::<chrono::DateTime<chrono::Utc>>() {
                        job = job.scheduled_at(dt);
                    }
                }

                let job_id = workers.enqueue(job).await?;

                let mut doc = crate::types::Document::new();
                doc.id = job_id.clone();
                doc.data
                    .insert("job_id".to_string(), crate::types::Value::String(job_id));
                doc.data.insert(
                    "job_type".to_string(),
                    crate::types::Value::String(job_type.clone()),
                );
                doc.data.insert(
                    "status".to_string(),
                    crate::types::Value::String("pending".to_string()),
                );

                Ok(MutationResult {
                    operation: "enqueueJob".to_string(),
                    collection: "__jobs".to_string(),
                    affected_count: 1,
                    returned_documents: vec![doc],
                })
            }
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
        // ContainsAny/ContainsAll don't have EventFilter equivalents; fall back to Contains
        AqlFilter::ContainsAny(f, v) | AqlFilter::ContainsAll(f, v) => Ok(EventFilter::Contains(
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
        AqlFilter::Matches(f, v) => {
            let pattern = match aql_value_to_db_value(v, vars)? {
                crate::types::Value::String(s) => s,
                other => other.to_string(),
            };
            let re = regex::Regex::new(&pattern).map_err(|e| {
                crate::error::AqlError::invalid_operation(format!("Invalid regex pattern: {}", e))
            })?;
            Ok(EventFilter::Matches(f.clone(), re))
        }
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
    db: &Aurora,
    intro: &ast::IntrospectionQuery,
) -> Result<ExecutionResult> {
    let names = db.list_collection_names();
    let want_fields = intro.fields.is_empty()
        || intro
            .fields
            .iter()
            .any(|f| f == "collections" || f == "fields");

    let documents: Vec<Document> = names
        .iter()
        .filter_map(|name| {
            // Skip internal system collections from introspection unless asked
            if name.starts_with('_') {
                return None;
            }
            let col = db.get_collection_definition(name).ok()?;
            let mut data = HashMap::new();
            data.insert("name".to_string(), Value::String(name.clone()));

            if want_fields {
                let field_list: Vec<Value> = col
                    .fields
                    .iter()
                    .map(|(fname, fdef)| {
                        let mut fdata = HashMap::new();
                        fdata.insert("name".to_string(), Value::String(fname.clone()));
                        fdata.insert(
                            "type".to_string(),
                            Value::String(fdef.field_type.to_string()),
                        );
                        fdata.insert("required".to_string(), Value::Bool(!fdef.nullable));
                        fdata.insert("indexed".to_string(), Value::Bool(fdef.indexed));
                        fdata.insert("unique".to_string(), Value::Bool(fdef.unique));
                        if !fdef.validations.is_empty() {
                            let vcons: Vec<Value> = fdef
                                .validations
                                .iter()
                                .map(|c| Value::String(format!("{:?}", c)))
                                .collect();
                            fdata.insert("validations".to_string(), Value::Array(vcons));
                        }
                        Value::Object(fdata)
                    })
                    .collect();
                data.insert("fields".to_string(), Value::Array(field_list));
            }

            Some(Document {
                id: name.clone(),
                data,
            })
        })
        .collect();

    let count = documents.len();
    Ok(ExecutionResult::Query(QueryResult {
        collection: "__schema".to_string(),
        documents,
        total_count: Some(count),
        deferred_fields: vec![],
        explain: None,
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
                    field_defs.push((field.name.as_str(), build_field_def(field)));
                }
                db.new_collection(name, field_defs).await?;
            }
            ast::SchemaOp::AlterCollection { name, actions } => {
                last_collection = name.clone();
                for action in actions {
                    match action {
                        ast::AlterAction::AddField { field, default } => {
                            let def = build_field_def(field);
                            db.add_field_to_schema(name, field.name.clone(), def)
                                .await?;
                            if let Some(default_val) = default {
                                let db_val = aql_value_to_db_value(default_val, &HashMap::new())?;
                                let docs = db.get_all_collection(name).await?;
                                for doc in docs {
                                    if !doc.data.contains_key(&field.name) {
                                        db.update_document(
                                            name,
                                            &doc.id,
                                            vec![(&field.name, db_val.clone())],
                                        )
                                        .await?;
                                    }
                                }
                            }
                        }
                        ast::AlterAction::DropField(field_name) => {
                            db.drop_field_from_schema(name, field_name.clone()).await?;
                        }
                        ast::AlterAction::RenameField { from, to } => {
                            db.rename_field_in_schema(name, from.clone(), to.clone())
                                .await?;
                            let docs = db.get_all_collection(name).await?;
                            for mut doc in docs {
                                if let Some(val) = doc.data.remove(from.as_str()) {
                                    doc.data.insert(to.clone(), val);
                                    let key = format!("{}:{}", name, doc.id);
                                    db.put(key, serde_json::to_vec(&doc)?, None).await?;
                                }
                            }
                        }
                        ast::AlterAction::ModifyField(field) => {
                            db.modify_field_in_schema(
                                name,
                                field.name.clone(),
                                build_field_def(field),
                            )
                            .await?;
                        }
                    }
                }
            }
            ast::SchemaOp::DropCollection { name, .. } => {
                db.drop_collection_schema(name).await?;
                last_collection = name.clone();
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

/// Parse @validate directive arguments into FieldValidationConstraints
fn parse_validate_directive(
    directive: &ast::Directive,
) -> Vec<crate::types::FieldValidationConstraint> {
    use crate::types::FieldValidationConstraint as FVC;
    let mut constraints = Vec::new();
    for arg in &directive.arguments {
        match arg.name.as_str() {
            "format" => {
                if let ast::Value::String(s) = &arg.value {
                    constraints.push(FVC::Format(s.clone()));
                }
            }
            "min" => {
                let n = match &arg.value {
                    ast::Value::Float(f) => Some(*f),
                    ast::Value::Int(i) => Some(*i as f64),
                    _ => None,
                };
                if let Some(n) = n {
                    constraints.push(FVC::Min(n));
                }
            }
            "max" => {
                let n = match &arg.value {
                    ast::Value::Float(f) => Some(*f),
                    ast::Value::Int(i) => Some(*i as f64),
                    _ => None,
                };
                if let Some(n) = n {
                    constraints.push(FVC::Max(n));
                }
            }
            "minLength" => {
                if let ast::Value::Int(i) = &arg.value {
                    constraints.push(FVC::MinLength(*i));
                }
            }
            "maxLength" => {
                if let ast::Value::Int(i) = &arg.value {
                    constraints.push(FVC::MaxLength(*i));
                }
            }
            "pattern" => {
                if let ast::Value::String(s) = &arg.value {
                    constraints.push(FVC::Pattern(s.clone()));
                }
            }
            _ => {}
        }
    }
    constraints
}

/// Build a FieldDefinition from a field's type + directives
fn build_field_def(field: &ast::FieldDef) -> FieldDefinition {
    let field_type = map_ast_type(&field.field_type);
    let mut indexed = false;
    let mut unique = false;
    let mut validations = Vec::new();
    for directive in &field.directives {
        match directive.name.as_str() {
            "indexed" | "index" => indexed = true,
            "unique" => {
                unique = true;
                indexed = true;
            }
            "primary" => {
                indexed = true;
                unique = true;
            }
            "validate" => validations.extend(parse_validate_directive(directive)),
            _ => {}
        }
    }
    FieldDefinition {
        field_type,
        unique,
        indexed,
        nullable: !field.field_type.is_required,
        validations,
    }
}

async fn execute_migration(
    db: &Aurora,
    migration: &ast::Migration,
    _options: &ExecutionOptions,
) -> Result<ExecutionResult> {
    let mut steps_applied = 0;
    let mut last_version = String::new();

    for step in &migration.steps {
        last_version = step.version.clone();

        if db.is_migration_applied(&step.version).await? {
            eprintln!(
                "[migration] version '{}' already applied — skipping",
                step.version
            );
            continue;
        }

        eprintln!("[migration] applying version '{}'", step.version);

        for action in &step.actions {
            match action {
                ast::MigrationAction::Schema(schema_op) => {
                    execute_single_schema_op(db, schema_op).await?;
                }
                ast::MigrationAction::DataMigration(dm) => {
                    execute_data_migration(db, dm).await?;
                }
            }
        }

        db.mark_migration_applied(&step.version).await?;
        steps_applied += 1;
        eprintln!("[migration] version '{}' applied", step.version);
    }

    let status = if steps_applied > 0 {
        "applied".to_string()
    } else {
        "skipped".to_string()
    };

    Ok(ExecutionResult::Migration(MigrationResult {
        version: last_version,
        steps_applied,
        status,
    }))
}

/// Execute a single SchemaOp — shared by execute_schema and execute_migration.
async fn execute_single_schema_op(db: &Aurora, op: &ast::SchemaOp) -> Result<()> {
    match op {
        ast::SchemaOp::DefineCollection {
            name,
            fields,
            if_not_exists,
            ..
        } => {
            if *if_not_exists && db.get_collection_definition(name).is_ok() {
                return Ok(());
            }
            let field_defs: Vec<(&str, FieldDefinition)> = fields
                .iter()
                .map(|f| (f.name.as_str(), build_field_def(f)))
                .collect();
            db.new_collection(name, field_defs).await?;
        }
        ast::SchemaOp::AlterCollection { name, actions } => {
            for action in actions {
                match action {
                    ast::AlterAction::AddField { field, default } => {
                        db.add_field_to_schema(name, field.name.clone(), build_field_def(field))
                            .await?;
                        if let Some(default_val) = default {
                            let db_val = aql_value_to_db_value(default_val, &HashMap::new())?;
                            let docs = db.get_all_collection(name).await?;
                            for doc in docs {
                                if !doc.data.contains_key(&field.name) {
                                    db.update_document(
                                        name,
                                        &doc.id,
                                        vec![(&field.name, db_val.clone())],
                                    )
                                    .await?;
                                }
                            }
                        }
                    }
                    ast::AlterAction::DropField(field_name) => {
                        db.drop_field_from_schema(name, field_name.clone()).await?;
                    }
                    ast::AlterAction::RenameField { from, to } => {
                        db.rename_field_in_schema(name, from.clone(), to.clone())
                            .await?;
                        // Rename the field key in every existing document (one read-modify-write per doc)
                        let docs = db.get_all_collection(name).await?;
                        for mut doc in docs {
                            if let Some(val) = doc.data.remove(from.as_str()) {
                                doc.data.insert(to.clone(), val);
                                let key = format!("{}:{}", name, doc.id);
                                db.put(key, serde_json::to_vec(&doc)?, None).await?;
                            }
                        }
                    }
                    ast::AlterAction::ModifyField(field) => {
                        db.modify_field_in_schema(name, field.name.clone(), build_field_def(field))
                            .await?;
                    }
                }
            }
        }
        ast::SchemaOp::DropCollection { name, if_exists } => {
            if *if_exists && db.get_collection_definition(name).is_err() {
                return Ok(());
            }
            db.drop_collection_schema(name).await?;
        }
    }
    Ok(())
}

/// Apply a `migrate data in <collection> { set field = expr where { ... } }` block.
async fn execute_data_migration(db: &Aurora, dm: &ast::DataMigration) -> Result<()> {
    let docs = db.get_all_collection(&dm.collection).await?;

    for doc in docs {
        for transform in &dm.transforms {
            let matches = match &transform.filter {
                Some(filter) => {
                    let compiled = compile_filter(filter)?;
                    matches_filter(&doc, &compiled, &HashMap::new())
                }
                None => true,
            };

            if matches {
                let new_value = eval_migration_expr(&transform.expression, &doc);
                let mut updates = HashMap::new();
                updates.insert(transform.field.clone(), new_value);
                db.aql_update_document(&dm.collection, &doc.id, updates)
                    .await?;
            }
        }
    }
    Ok(())
}

/// Evaluate a migration expression string to a `Value`.
///
/// Supported forms (evaluated in order):
/// - String literal:  `"hello"` → `Value::String("hello")`
/// - Boolean:         `true` / `false`
/// - Null:            `null`
/// - Integer:         `42`
/// - Float:           `3.14`
/// - Field reference: `field_name` (looks up the field in the current document)
fn eval_migration_expr(expr: &str, doc: &Document) -> Value {
    let expr = expr.trim();

    if expr.starts_with('"') && expr.ends_with('"') && expr.len() >= 2 {
        return Value::String(expr[1..expr.len() - 1].to_string());
    }
    if expr == "true" {
        return Value::Bool(true);
    }
    if expr == "false" {
        return Value::Bool(false);
    }
    if expr == "null" {
        return Value::Null;
    }
    if let Ok(n) = expr.parse::<i64>() {
        return Value::Int(n);
    }
    if let Ok(f) = expr.parse::<f64>() {
        return Value::Float(f);
    }
    if let Some(v) = doc.data.get(expr) {
        return v.clone();
    }

    Value::Null
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
                    // Formal syntax: { field: "fieldname", direction: ASC|DESC }
                    if let (Some(ast::Value::String(field_name)), Some(dir_val)) =
                        (obj.get("field"), obj.get("direction"))
                    {
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
                            field: field_name.clone(),
                            direction,
                        });
                    } else {
                        // Shorthand: { fieldName: "ASC", fieldName2: "DESC" }
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
        deferred_fields: vec![],
        explain: None,
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
        // Field array must contain at least one of the provided values
        CompiledFilter::ContainsAny(f, v) => {
            if let (Some(Value::Array(field_arr)), Ok(Value::Array(check_arr))) =
                (doc.data.get(f), aql_value_to_db_value(v, vars))
            {
                check_arr.iter().any(|item| field_arr.contains(item))
            } else {
                false
            }
        }
        // Field array must contain all of the provided values
        CompiledFilter::ContainsAll(f, v) => {
            if let (Some(Value::Array(field_arr)), Ok(Value::Array(check_arr))) =
                (doc.data.get(f), aql_value_to_db_value(v, vars))
            {
                check_arr.iter().all(|item| field_arr.contains(item))
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

/// Apply an update modifier object to an existing field value.
/// Modifiers: `{ increment: N }`, `{ decrement: N }`, `{ push: V }`,
/// `{ pull: V }`, `{ addToSet: V }`.
/// Any non-modifier object is returned as-is (plain field replace).
fn apply_field_modifier(existing: Option<&Value>, new_val: &Value) -> Value {
    if let Value::Object(modifier) = new_val {
        if let Some(delta) = modifier.get("increment") {
            match (existing, delta) {
                (Some(Value::Int(c)), Value::Int(d)) => return Value::Int(c + d),
                (Some(Value::Float(c)), Value::Float(d)) => return Value::Float(c + d),
                (Some(Value::Int(c)), Value::Float(d)) => return Value::Float(*c as f64 + d),
                _ => {}
            }
        }
        if let Some(delta) = modifier.get("decrement") {
            match (existing, delta) {
                (Some(Value::Int(c)), Value::Int(d)) => return Value::Int(c - d),
                (Some(Value::Float(c)), Value::Float(d)) => return Value::Float(c - d),
                (Some(Value::Int(c)), Value::Float(d)) => return Value::Float(*c as f64 - d),
                _ => {}
            }
        }
        if let Some(item) = modifier.get("push") {
            if let Some(Value::Array(mut arr)) = existing.cloned() {
                arr.push(item.clone());
                return Value::Array(arr);
            }
            return Value::Array(vec![item.clone()]);
        }
        if let Some(item) = modifier.get("pull") {
            if let Some(Value::Array(arr)) = existing {
                let filtered: Vec<Value> = arr.iter().filter(|v| *v != item).cloned().collect();
                return Value::Array(filtered);
            }
            return Value::Array(vec![]);
        }
        if let Some(item) = modifier.get("addToSet") {
            if let Some(Value::Array(mut arr)) = existing.cloned() {
                if !arr.contains(item) {
                    arr.push(item.clone());
                }
                return Value::Array(arr);
            }
            return Value::Array(vec![item.clone()]);
        }
    }
    new_val.clone()
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

pub fn apply_projection(doc: Document, fields: &[ast::Field]) -> Document {
    let (projected, _) = apply_projection_and_defer(doc, fields);
    projected
}

/// Apply projections, resolving lookup fields from foreign collections.
/// Returns (projected_doc, deferred_field_names).
async fn apply_projection_with_lookups(
    db: &Aurora,
    mut doc: Document,
    fields: &[ast::Field],
    vars: &HashMap<String, ast::Value>,
) -> Result<(Document, Vec<String>)> {
    if fields.is_empty() {
        return Ok((doc, vec![]));
    }
    let mut proj = HashMap::new();
    let mut deferred = Vec::new();

    for f in fields {
        // @defer
        if f.directives.iter().any(|d| d.name == "defer") {
            deferred.push(f.alias.as_ref().unwrap_or(&f.name).clone());
            continue;
        }

        // Lookup field: has collection + localField + foreignField arguments
        let coll_arg = f.arguments.iter().find(|a| a.name == "collection");
        let local_arg = f.arguments.iter().find(|a| a.name == "localField");
        let foreign_arg = f.arguments.iter().find(|a| a.name == "foreignField");

        if let (Some(c), Some(lf), Some(ff)) = (coll_arg, local_arg, foreign_arg) {
            // Resolve variable references in lookup arguments
            let c_val = resolve_if_variable(&c.value, vars);
            let lf_val = resolve_if_variable(&lf.value, vars);
            let ff_val = resolve_if_variable(&ff.value, vars);
            let foreign_coll = if let ast::Value::String(s) = c_val {
                s.as_str()
            } else {
                continue;
            };
            let local_field = if let ast::Value::String(s) = lf_val {
                s.as_str()
            } else {
                continue;
            };
            let foreign_field = if let ast::Value::String(s) = ff_val {
                s.as_str()
            } else {
                continue;
            };

            // Get the local field value to match against the foreign collection
            let local_val = doc.data.get(local_field).cloned().or_else(|| {
                if local_field == "id" {
                    Some(Value::String(doc.id.clone()))
                } else {
                    None
                }
            });

            if let Some(match_val) = local_val {
                // Optional filter — deeply resolve variables inside it before compiling
                let extra_filter = f
                    .arguments
                    .iter()
                    .find(|a| a.name == "where")
                    .and_then(|a| {
                        let resolved = resolve_ast_deep(&a.value, vars);
                        value_to_filter(&resolved).ok()
                    });

                let vars_arc = Arc::new(vars.clone());
                let foreign_docs = db.scan_and_filter(
                    foreign_coll,
                    |fdoc| {
                        let field_match = fdoc
                            .data
                            .get(foreign_field)
                            .map(|v| values_equal_db(v, &match_val))
                            .unwrap_or(foreign_field == "id" && fdoc.id == match_val.to_string());
                        if !field_match {
                            return false;
                        }
                        if let Some(ref ef) = extra_filter {
                            let compiled = compile_filter(ef)
                                .unwrap_or(CompiledFilter::Eq("_".into(), ast::Value::Null));
                            matches_filter(fdoc, &compiled, &vars_arc)
                        } else {
                            true
                        }
                    },
                    None,
                )?;

                // Apply sub-projection to foreign docs
                let sub_projected: Vec<Value> = if f.selection_set.is_empty() {
                    foreign_docs
                        .into_iter()
                        .map(|fd| Value::Object(fd.data))
                        .collect()
                } else {
                    let sub_fields: Vec<ast::Field> = f
                        .selection_set
                        .iter()
                        .filter_map(|sel| {
                            if let Selection::Field(sf) = sel {
                                Some(sf.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    foreign_docs
                        .into_iter()
                        .map(|fd| {
                            let (proj_fd, _) = apply_projection_and_defer(fd, &sub_fields);
                            Value::Object(proj_fd.data)
                        })
                        .collect()
                };

                let alias = f.alias.as_ref().unwrap_or(&f.name).clone();
                proj.insert(alias, Value::Array(sub_projected));
            }
            continue;
        }

        // Computed field
        if f.name == "__compute__" {
            let alias = f.alias.as_deref().unwrap_or("computed");
            if let Some(expr) = f.arguments.iter().find(|a| a.name == "expr") {
                let resolved_expr = resolve_if_variable(&expr.value, vars);
                if let ast::Value::String(template) = resolved_expr {
                    let result = eval_template(template, &doc.data);
                    proj.insert(alias.to_string(), Value::String(result));
                }
            }
            continue;
        }

        // Normal field
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
    Ok((doc, deferred))
}

fn values_equal_db(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(s1), Value::String(s2)) => s1 == s2,
        (Value::Int(i1), Value::Int(i2)) => i1 == i2,
        (Value::Float(f1), Value::Float(f2)) => (f1 - f2).abs() < f64::EPSILON,
        (Value::Bool(b1), Value::Bool(b2)) => b1 == b2,
        _ => false,
    }
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

/// Convert AQL AST value to serde_json::Value for job payloads
fn aql_value_to_json(v: &ast::Value) -> serde_json::Value {
    match v {
        ast::Value::Null => serde_json::Value::Null,
        ast::Value::Boolean(b) => serde_json::Value::Bool(*b),
        ast::Value::Int(i) => serde_json::Value::Number((*i).into()),
        ast::Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ast::Value::String(s) | ast::Value::Enum(s) => serde_json::Value::String(s.clone()),
        ast::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(aql_value_to_json).collect())
        }
        ast::Value::Object(obj) => serde_json::Value::Object(
            obj.iter()
                .map(|(k, v)| (k.clone(), aql_value_to_json(v)))
                .collect(),
        ),
        ast::Value::Variable(_) => serde_json::Value::Null,
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
                                "containsAny" => {
                                    fs.push(AqlFilter::ContainsAny(field.to_string(), ov.clone()))
                                }
                                "containsAll" => {
                                    fs.push(AqlFilter::ContainsAll(field.to_string(), ov.clone()))
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

/// Recursively resolve all variable references inside an AST value.
/// Unlike `resolve_value` (which only handles a top-level `$var`), this walks
/// into objects and arrays so nested references like
/// `{ query: $term }` or `{ status: { eq: $activeStatus } }` are fully resolved.
fn resolve_ast_deep(v: &ast::Value, vars: &HashMap<String, ast::Value>) -> ast::Value {
    match v {
        ast::Value::Variable(n) => vars.get(n).cloned().unwrap_or(ast::Value::Null),
        ast::Value::Object(m) => ast::Value::Object(
            m.iter()
                .map(|(k, val)| (k.clone(), resolve_ast_deep(val, vars)))
                .collect(),
        ),
        ast::Value::Array(arr) => {
            ast::Value::Array(arr.iter().map(|val| resolve_ast_deep(val, vars)).collect())
        }
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
                    ..Default::default()
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
