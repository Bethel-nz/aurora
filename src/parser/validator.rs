//! AQL Validator - Validates parsed AQL documents before execution
//!
//! Performs:
//! - Type checking against schema
//! - Variable resolution
//! - Filter operator validation
//! - Collection and field existence checks

use crate::types::{Collection, FieldType};
use super::ast::{self, Document, Query, Mutation, Subscription, Field, Filter, Value};
use std::collections::HashMap;
use std::fmt;

/// Error codes for validation failures
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Referenced collection does not exist
    UnknownCollection,
    /// Referenced field does not exist in collection
    UnknownField,
    /// Value type doesn't match field type
    TypeMismatch,
    /// Required variable not provided
    MissingRequiredVariable,
    /// Optional variable used without default value
    MissingOptionalVariable,
    /// Filter operator not valid for field type
    InvalidFilterOperator,
    /// Duplicate alias in selection set
    DuplicateAlias,
    /// Invalid argument provided
    InvalidArgument,
    /// Unknown directive
    UnknownDirective,
}

/// Validation error with context
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub code: ErrorCode,
    pub message: String,
    pub path: Option<String>,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.path {
            Some(path) => write!(f, "[{}] {}", path, self.message),
            None => write!(f, "{}", self.message),
        }
    }
}

impl std::error::Error for ValidationError {}

impl ValidationError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            path: None,
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }
}

/// Validation result type
pub type ValidationResult = Result<(), Vec<ValidationError>>;

/// Schema provider trait for validation
/// Allows validation without direct Aurora dependency
pub trait SchemaProvider {
    /// Get a collection definition by name
    fn get_collection(&self, name: &str) -> Option<&Collection>;
    
    /// Check if a collection exists
    fn collection_exists(&self, name: &str) -> bool {
        self.get_collection(name).is_some()
    }
}

/// Simple in-memory schema for testing
#[derive(Debug, Default)]
pub struct InMemorySchema {
    collections: HashMap<String, Collection>,
}

impl InMemorySchema {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_collection(&mut self, collection: Collection) {
        self.collections.insert(collection.name.clone(), collection);
    }
}

impl SchemaProvider for InMemorySchema {
    fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }
}

/// Validation context holding schema and variable values
pub struct ValidationContext<'a, S: SchemaProvider> {
    pub schema: &'a S,
    pub variables: HashMap<String, ast::Value>,
    pub errors: Vec<ValidationError>,
    current_path: Vec<String>,
}

impl<'a, S: SchemaProvider> ValidationContext<'a, S> {
    pub fn new(schema: &'a S) -> Self {
        Self {
            schema,
            variables: HashMap::new(),
            errors: Vec::new(),
            current_path: Vec::new(),
        }
    }

    pub fn with_variables(mut self, variables: HashMap<String, ast::Value>) -> Self {
        self.variables = variables;
        self
    }

    fn push_path(&mut self, segment: &str) {
        self.current_path.push(segment.to_string());
    }

    fn pop_path(&mut self) {
        self.current_path.pop();
    }

    fn current_path_string(&self) -> Option<String> {
        if self.current_path.is_empty() {
            None
        } else {
            Some(self.current_path.join("."))
        }
    }

    fn add_error(&mut self, code: ErrorCode, message: impl Into<String>) {
        let mut error = ValidationError::new(code, message);
        error.path = self.current_path_string();
        self.errors.push(error);
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn into_result(self) -> ValidationResult {
        if self.errors.is_empty() {
            Ok(())
        } else {
            Err(self.errors)
        }
    }
}

/// Validate a complete AQL document
pub fn validate_document<S: SchemaProvider>(
    doc: &Document,
    schema: &S,
    variables: HashMap<String, ast::Value>,
) -> ValidationResult {
    let mut ctx = ValidationContext::new(schema).with_variables(variables);

    for (i, op) in doc.operations.iter().enumerate() {
        ctx.push_path(&format!("operation[{}]", i));
        match op {
            ast::Operation::Query(query) => validate_query(query, &mut ctx),
            ast::Operation::Mutation(mutation) => validate_mutation(mutation, &mut ctx),
            ast::Operation::Subscription(sub) => validate_subscription(sub, &mut ctx),
            ast::Operation::Schema(_) => {} // Schema definitions don't need validation
            ast::Operation::Migration(_) => {}
        }
        ctx.pop_path();
    }

    ctx.into_result()
}

/// Validate a query operation
fn validate_query<S: SchemaProvider>(query: &Query, ctx: &mut ValidationContext<'_, S>) {
    if let Some(name) = &query.name {
        ctx.push_path(name);
    }

    // Validate variable definitions
    validate_variable_definitions(&query.variable_definitions, ctx);

    // Validate selection set
    for field in &query.selection_set {
        validate_field(field, ctx);
    }

    if query.name.is_some() {
        ctx.pop_path();
    }
}

/// Validate a mutation operation
fn validate_mutation<S: SchemaProvider>(mutation: &Mutation, ctx: &mut ValidationContext<'_, S>) {
    if let Some(name) = &mutation.name {
        ctx.push_path(name);
    }

    // Validate variable definitions
    validate_variable_definitions(&mutation.variable_definitions, ctx);

    // Validate each mutation operation
    for (i, op) in mutation.operations.iter().enumerate() {
        ctx.push_path(&format!("mutation[{}]", i));
        validate_mutation_operation(op, ctx);
        ctx.pop_path();
    }

    if mutation.name.is_some() {
        ctx.pop_path();
    }
}

/// Validate a subscription operation
fn validate_subscription<S: SchemaProvider>(sub: &Subscription, ctx: &mut ValidationContext<'_, S>) {
    if let Some(name) = &sub.name {
        ctx.push_path(name);
    }

    // Validate variable definitions
    validate_variable_definitions(&sub.variable_definitions, ctx);

    // Validate selection set
    for field in &sub.selection_set {
        validate_field(field, ctx);
    }

    if sub.name.is_some() {
        ctx.pop_path();
    }
}

/// Validate variable definitions
fn validate_variable_definitions<S: SchemaProvider>(
    definitions: &[ast::VariableDefinition],
    ctx: &mut ValidationContext<'_, S>,
) {
    for def in definitions {
        let var_name = &def.name;
        
        // Check if required variable is provided
        if def.var_type.is_required && def.default_value.is_none() {
            if !ctx.variables.contains_key(var_name) {
                ctx.add_error(
                    ErrorCode::MissingRequiredVariable,
                    format!("Required variable '{}' is not provided", var_name),
                );
            }
        }
    }
}

/// Validate a field selection
fn validate_field<S: SchemaProvider>(field: &Field, ctx: &mut ValidationContext<'_, S>) {
    let field_name = field.alias.as_ref().unwrap_or(&field.name);
    ctx.push_path(field_name);

    // Check for collection reference (top-level field)
    if field.selection_set.is_empty() {
        // Leaf field - no further validation needed for now
    } else {
        // This is a collection query
        let collection_name = &field.name;
        
        if !ctx.schema.collection_exists(collection_name) {
            ctx.add_error(
                ErrorCode::UnknownCollection,
                format!("Collection '{}' does not exist", collection_name),
            );
        } else {
            // Validate nested fields against collection schema
            if let Some(collection) = ctx.schema.get_collection(collection_name) {
                validate_selection_set(&field.selection_set, collection, ctx);
            }
        }

        // Validate filter if present
        for arg in &field.arguments {
            if arg.name == "where" || arg.name == "filter" {
                if let Some(filter) = extract_filter_from_value(&arg.value) {
                    if let Some(collection) = ctx.schema.get_collection(collection_name) {
                        validate_filter(&filter, collection, ctx);
                    }
                }
            }
        }
    }

    ctx.pop_path();
}

/// Validate selection set against collection schema
fn validate_selection_set<S: SchemaProvider>(
    fields: &[Field],
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    let mut aliases_seen: HashMap<String, bool> = HashMap::new();

    for field in fields {
        let display_name = field.alias.as_ref().unwrap_or(&field.name);
        
        // Check for duplicate aliases
        if aliases_seen.contains_key(display_name) {
            ctx.add_error(
                ErrorCode::DuplicateAlias,
                format!("Duplicate field/alias '{}' in selection", display_name),
            );
        }
        aliases_seen.insert(display_name.clone(), true);

        // Check if field exists in collection (skip special fields)
        let field_name = &field.name;
        if !field_name.starts_with("__") && field_name != "id" {
            if !collection.fields.contains_key(field_name) {
                ctx.add_error(
                    ErrorCode::UnknownField,
                    format!("Field '{}' does not exist in collection '{}'", field_name, collection.name),
                );
            }
        }
    }
}

/// Validate a mutation operation
fn validate_mutation_operation<S: SchemaProvider>(
    op: &ast::MutationOperation,
    ctx: &mut ValidationContext<'_, S>,
) {
    match &op.operation {
        ast::MutationOp::Insert { collection, .. } |
        ast::MutationOp::Update { collection, .. } |
        ast::MutationOp::Upsert { collection, .. } |
        ast::MutationOp::Delete { collection, .. } => {
            if !ctx.schema.collection_exists(collection) {
                ctx.add_error(
                    ErrorCode::UnknownCollection,
                    format!("Collection '{}' does not exist", collection),
                );
            }
        }
        ast::MutationOp::InsertMany { collection, .. } => {
            if !ctx.schema.collection_exists(collection) {
                ctx.add_error(
                    ErrorCode::UnknownCollection,
                    format!("Collection '{}' does not exist", collection),
                );
            }
        }
        ast::MutationOp::EnqueueJob { .. } => {
            // Job validation - minimal for now
        }
        ast::MutationOp::Transaction { operations } => {
            for (i, inner_op) in operations.iter().enumerate() {
                ctx.push_path(&format!("tx[{}]", i));
                validate_mutation_operation(inner_op, ctx);
                ctx.pop_path();
            }
        }
    }
}

/// Validate a filter expression
fn validate_filter<S: SchemaProvider>(
    filter: &Filter,
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    match filter {
        Filter::Eq(field, value) |
        Filter::Ne(field, value) |
        Filter::Gt(field, value) |
        Filter::Gte(field, value) |
        Filter::Lt(field, value) |
        Filter::Lte(field, value) => {
            validate_filter_field(field, value, collection, ctx);
        }
        Filter::In(field, value) |
        Filter::NotIn(field, value) => {
            // In/NotIn expects an array value
            if !matches!(value, Value::Array(_)) {
                ctx.add_error(
                    ErrorCode::TypeMismatch,
                    format!("Filter 'in'/'notIn' on '{}' requires an array value", field),
                );
            }
            validate_filter_field_exists(field, collection, ctx);
        }
        Filter::Contains(field, _) |
        Filter::StartsWith(field, _) |
        Filter::EndsWith(field, _) |
        Filter::Matches(field, _) => {
            // String operations - check field is a string type
            if let Some(field_def) = collection.fields.get(field) {
                if field_def.field_type != FieldType::String {
                    ctx.add_error(
                        ErrorCode::InvalidFilterOperator,
                        format!("String operator on non-string field '{}'", field),
                    );
                }
            } else {
                validate_filter_field_exists(field, collection, ctx);
            }
        }
        Filter::IsNull(field) |
        Filter::IsNotNull(field) => {
            validate_filter_field_exists(field, collection, ctx);
        }
        Filter::And(filters) |
        Filter::Or(filters) => {
            for f in filters {
                validate_filter(f, collection, ctx);
            }
        }
        Filter::Not(inner) => {
            validate_filter(inner, collection, ctx);
        }
    }
}

/// Validate a filter field exists
fn validate_filter_field_exists<S: SchemaProvider>(
    field: &str,
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    if field != "id" && !collection.fields.contains_key(field) {
        ctx.add_error(
            ErrorCode::UnknownField,
            format!("Filter field '{}' does not exist in collection '{}'", field, collection.name),
        );
    }
}

/// Validate a filter field with value type checking
fn validate_filter_field<S: SchemaProvider>(
    field: &str,
    value: &Value,
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    if field == "id" {
        return; // id field always exists
    }

    if let Some(field_def) = collection.fields.get(field) {
        // Check type compatibility
        if !is_type_compatible(&field_def.field_type, value) {
            ctx.add_error(
                ErrorCode::TypeMismatch,
                format!(
                    "Type mismatch: field '{}' expects {:?}, got {:?}",
                    field, field_def.field_type, value_type_name(value)
                ),
            );
        }
    } else {
        ctx.add_error(
            ErrorCode::UnknownField,
            format!("Filter field '{}' does not exist in collection '{}'", field, collection.name),
        );
    }
}

/// Check if a value is compatible with a field type
fn is_type_compatible(field_type: &FieldType, value: &Value) -> bool {
    match (field_type, value) {
        (_, Value::Null) => true, // Null is compatible with any type
        (_, Value::Variable(_)) => true, // Variables are resolved later
        (FieldType::String, Value::String(_)) => true,
        (FieldType::Int, Value::Int(_)) => true,
        (FieldType::Float, Value::Float(_)) => true,
        (FieldType::Float, Value::Int(_)) => true, // Int can be used where Float expected
        (FieldType::Bool, Value::Boolean(_)) => true,
        (FieldType::Array, Value::Array(_)) => true,
        (FieldType::Object, Value::Object(_)) => true,
        (FieldType::Any, _) => true, // Any type accepts all values
        (FieldType::Uuid, Value::String(_)) => true, // UUIDs are often passed as strings
        _ => false,
    }
}

/// Get a human-readable type name for a value
fn value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Boolean(_) => "boolean",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
        Value::Variable(_) => "variable",
        Value::Enum(_) => "enum",
    }
}

/// Extract a Filter from an AST Value (for parsing where arguments)
fn extract_filter_from_value(_value: &Value) -> Option<Filter> {
    // This is a placeholder - actual filter extraction would parse the where object
    // For now, filters are constructed during parsing in mod.rs
    None
}

/// Resolve variables in a document, replacing Value::Variable with actual values
pub fn resolve_variables(
    doc: &mut Document,
    variables: &HashMap<String, ast::Value>,
) -> Result<(), ValidationError> {
    for op in &mut doc.operations {
        match op {
            ast::Operation::Query(query) => {
                resolve_in_fields(&mut query.selection_set, variables)?;
            }
            ast::Operation::Mutation(mutation) => {
                for mut_op in &mut mutation.operations {
                    resolve_in_mutation_op(mut_op, variables)?;
                }
            }
            ast::Operation::Subscription(sub) => {
                resolve_in_fields(&mut sub.selection_set, variables)?;
            }
            ast::Operation::Schema(_) => {}
            ast::Operation::Migration(_) => {}
        }
    }
    Ok(())
}

fn resolve_in_fields(
    fields: &mut [Field],
    variables: &HashMap<String, ast::Value>,
) -> Result<(), ValidationError> {
    for field in fields {
        // Resolve variables in arguments
        for arg in &mut field.arguments {
            resolve_in_value(&mut arg.value, variables)?;
        }
        // Recursively resolve in nested selection
        resolve_in_fields(&mut field.selection_set, variables)?;
    }
    Ok(())
}

fn resolve_in_mutation_op(
    op: &mut ast::MutationOperation,
    variables: &HashMap<String, ast::Value>,
) -> Result<(), ValidationError> {
    match &mut op.operation {
        ast::MutationOp::Insert { data, .. } |
        ast::MutationOp::Update { data, .. } |
        ast::MutationOp::Upsert { data, .. } => {
            resolve_in_value(data, variables)?;
        }
        ast::MutationOp::InsertMany { data, .. } => {
            for item in data {
                resolve_in_value(item, variables)?;
            }
        }
        ast::MutationOp::Delete { .. } => {}
        ast::MutationOp::EnqueueJob { payload, .. } => {
            resolve_in_value(payload, variables)?;
        }
        ast::MutationOp::Transaction { operations } => {
            for inner in operations {
                resolve_in_mutation_op(inner, variables)?;
            }
        }
    }
    resolve_in_fields(&mut op.selection_set, variables)
}

fn resolve_in_value(
    value: &mut Value,
    variables: &HashMap<String, ast::Value>,
) -> Result<(), ValidationError> {
    match value {
        Value::Variable(name) => {
            if let Some(resolved) = variables.get(name) {
                *value = resolved.clone();
            } else {
                return Err(ValidationError::new(
                    ErrorCode::MissingRequiredVariable,
                    format!("Variable '{}' is not provided", name),
                ));
            }
        }
        Value::Array(items) => {
            for item in items {
                resolve_in_value(item, variables)?;
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                resolve_in_value(v, variables)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{FieldDefinition};

    fn create_test_schema() -> InMemorySchema {
        let mut schema = InMemorySchema::new();
        
        let mut users_fields = HashMap::new();
        users_fields.insert("name".to_string(), FieldDefinition {
            field_type: FieldType::String,
            unique: false,
            indexed: false,
        });
        users_fields.insert("email".to_string(), FieldDefinition {
            field_type: FieldType::String,
            unique: true,
            indexed: true,
        });
        users_fields.insert("age".to_string(), FieldDefinition {
            field_type: FieldType::Int,
            unique: false,
            indexed: false,
        });
        users_fields.insert("active".to_string(), FieldDefinition {
            field_type: FieldType::Bool,
            unique: false,
            indexed: false,
        });

        schema.add_collection(Collection {
            name: "users".to_string(),
            fields: users_fields,
        });

        schema
    }

    #[test]
    fn test_validate_unknown_collection() {
        let schema = create_test_schema();
        let doc = Document {
            operations: vec![
                ast::Operation::Query(Query {
                    name: None,
                    variable_definitions: vec![],
                    directives: vec![],
                    selection_set: vec![Field {
                        alias: None,
                        name: "nonexistent".to_string(),
                        arguments: vec![],
                        directives: vec![],
                        selection_set: vec![Field {
                            alias: None,
                            name: "id".to_string(),
                            arguments: vec![],
                            directives: vec![],
                            selection_set: vec![],
                        }],
                    }],
                    variables_values: HashMap::new(),
                }),
            ],
        };

        let result = validate_document(&doc, &schema, HashMap::new());
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.code == ErrorCode::UnknownCollection));
    }

    #[test]
    fn test_validate_unknown_field() {
        let schema = create_test_schema();
        let doc = Document {
            operations: vec![
                ast::Operation::Query(Query {
                    name: None,
                    variable_definitions: vec![],
                    directives: vec![],
                    selection_set: vec![Field {
                        alias: None,
                        name: "users".to_string(),
                        arguments: vec![],
                        directives: vec![],
                        selection_set: vec![Field {
                            alias: None,
                            name: "nonexistent_field".to_string(),
                            arguments: vec![],
                            directives: vec![],
                            selection_set: vec![],
                        }],
                    }],
                    variables_values: HashMap::new(),
                }),
            ],
        };

        let result = validate_document(&doc, &schema, HashMap::new());
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.code == ErrorCode::UnknownField));
    }

    #[test]
    fn test_validate_missing_required_variable() {
        let schema = create_test_schema();
        let doc = Document {
            operations: vec![
                ast::Operation::Query(Query {
                    name: Some("GetUsers".to_string()),
                    variable_definitions: vec![ast::VariableDefinition {
                        name: "minAge".to_string(),
                        var_type: ast::TypeAnnotation {
                            name: "Int".to_string(),
                            is_array: false,
                            is_required: true,
                        },
                        default_value: None,
                    }],
                    directives: vec![],
                    selection_set: vec![],
                    variables_values: HashMap::new(),
                }),
            ],
        };

        // No variables provided
        let result = validate_document(&doc, &schema, HashMap::new());
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.code == ErrorCode::MissingRequiredVariable));
    }

    #[test]
    fn test_validate_valid_query() {
        let schema = create_test_schema();
        let doc = Document {
            operations: vec![
                ast::Operation::Query(Query {
                    name: Some("GetUsers".to_string()),
                    variable_definitions: vec![],
                    directives: vec![],
                    selection_set: vec![Field {
                        alias: None,
                        name: "users".to_string(),
                        arguments: vec![],
                        directives: vec![],
                        selection_set: vec![
                            Field {
                                alias: None,
                                name: "id".to_string(),
                                arguments: vec![],
                                directives: vec![],
                                selection_set: vec![],
                            },
                            Field {
                                alias: None,
                                name: "name".to_string(),
                                arguments: vec![],
                                directives: vec![],
                                selection_set: vec![],
                            },
                            Field {
                                alias: None,
                                name: "email".to_string(),
                                arguments: vec![],
                                directives: vec![],
                                selection_set: vec![],
                            },
                        ],
                    }],
                    variables_values: HashMap::new(),
                }),
            ],
        };

        let result = validate_document(&doc, &schema, HashMap::new());
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_filter_type_mismatch() {
        let schema = create_test_schema();
        let collection = schema.get_collection("users").unwrap();
        let mut ctx = ValidationContext::new(&schema);

        // age is Int, but we're filtering with a string
        let filter = Filter::Eq("age".to_string(), Value::String("not a number".to_string()));
        validate_filter(&filter, collection, &mut ctx);

        assert!(ctx.has_errors());
        assert!(ctx.errors.iter().any(|e| e.code == ErrorCode::TypeMismatch));
    }

    #[test]
    fn test_validate_filter_string_operator_on_int() {
        let schema = create_test_schema();
        let collection = schema.get_collection("users").unwrap();
        let mut ctx = ValidationContext::new(&schema);

        // contains on int field should fail
        let filter = Filter::Contains("age".to_string(), Value::String("10".to_string()));
        validate_filter(&filter, collection, &mut ctx);

        assert!(ctx.has_errors());
        assert!(ctx.errors.iter().any(|e| e.code == ErrorCode::InvalidFilterOperator));
    }

    #[test]
    fn test_resolve_variables() {
        let mut doc = Document {
            operations: vec![
                ast::Operation::Query(Query {
                    name: None,
                    variable_definitions: vec![],
                    directives: vec![],
                    selection_set: vec![Field {
                        alias: None,
                        name: "users".to_string(),
                        arguments: vec![ast::Argument {
                            name: "limit".to_string(),
                            value: Value::Variable("pageSize".to_string()),
                        }],
                        directives: vec![],
                        selection_set: vec![],
                    }],
                    variables_values: HashMap::new(),
                }),
            ],
        };

        let mut vars = HashMap::new();
        vars.insert("pageSize".to_string(), Value::Int(10));

        let result = resolve_variables(&mut doc, &vars);
        assert!(result.is_ok());

        // Check that variable was resolved
        if let ast::Operation::Query(query) = &doc.operations[0] {
            let arg = &query.selection_set[0].arguments[0];
            assert!(matches!(arg.value, Value::Int(10)));
        } else {
            panic!("Expected Query operation");
        }
    }
}
