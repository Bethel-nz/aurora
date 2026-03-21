//! AQL Validator - Validates parsed AQL documents before execution
//!
//! Performs:
//! - Type checking against schema
//! - Variable resolution
//! - Filter operator validation
//! - Collection and field existence checks

use super::ast::{
    self, Document, Field, Filter, FragmentDef, Mutation, Query, Selection, Subscription, Value,
};
use crate::types::{Collection, FieldType, ScalarType};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// Error codes for validation failures
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    /// Referenced collection does not exist
    UnknownCollection,
    /// Referenced field does not exist in collection
    UnknownField,
    /// Explicitly invalid input (e.g. null where not allowed)
    InvalidInput,
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
    /// Unknown fragment
    UnknownFragment,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorCode::UnknownCollection => write!(f, "UnknownCollection"),
            ErrorCode::UnknownField => write!(f, "UnknownField"),
            ErrorCode::InvalidInput => write!(f, "InvalidInput"),
            ErrorCode::TypeMismatch => write!(f, "TypeMismatch"),
            ErrorCode::MissingRequiredVariable => write!(f, "MissingRequiredVariable"),
            ErrorCode::MissingOptionalVariable => write!(f, "MissingOptionalVariable"),
            ErrorCode::InvalidFilterOperator => write!(f, "InvalidFilterOperator"),
            ErrorCode::DuplicateAlias => write!(f, "DuplicateAlias"),
            ErrorCode::InvalidArgument => write!(f, "InvalidArgument"),
            ErrorCode::UnknownDirective => write!(f, "UnknownDirective"),
            ErrorCode::UnknownFragment => write!(f, "UnknownFragment"),
        }
    }
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
    pub fragments: HashMap<String, &'a FragmentDef>,
    pub errors: Vec<ValidationError>,
    current_path: Vec<String>,
    validating_fragments: HashSet<String>,
}

impl<'a, S: SchemaProvider> ValidationContext<'a, S> {
    pub fn new(schema: &'a S) -> Self {
        Self {
            schema,
            variables: HashMap::new(),
            fragments: HashMap::new(),
            errors: Vec::new(),
            current_path: Vec::new(),
            validating_fragments: HashSet::new(),
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

    // First pass: Collect fragment definitions
    for op in &doc.operations {
        if let ast::Operation::FragmentDefinition(frag) = op {
            if ctx.fragments.insert(frag.name.clone(), frag).is_some() {
                ctx.add_error(
                    ErrorCode::InvalidInput,
                    format!("Duplicate fragment definition '{}'", frag.name),
                );
            }
        }
    }

    for (i, op) in doc.operations.iter().enumerate() {
        ctx.push_path(&format!("operation[{}]", i));
        match op {
            ast::Operation::Query(query) => validate_query(query, &mut ctx),
            ast::Operation::Mutation(mutation) => validate_mutation(mutation, &mut ctx),
            ast::Operation::Subscription(sub) => validate_subscription(sub, &mut ctx),
            ast::Operation::Schema(_) => {} // Schema definitions don't need validation
            ast::Operation::Migration(_) => {}
            ast::Operation::FragmentDefinition(_) => {} // Fragment definitions validated when used
            ast::Operation::Introspection(_) => {}      // Introspection is always valid
            ast::Operation::Handler(_) => {}            // Handler definitions validated separately
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
    for selection in &query.selection_set {
        match selection {
            Selection::Field(field) => {
                validate_field(field, None, ctx);
            }
            Selection::InlineFragment(inline) => {
                validate_inline_fragment(inline, None, ctx);
            }
            Selection::FragmentSpread(fragment_name) => {
                // Fragment spreads at query root are invalid — their fields would be
                // interpreted as collection names by the planner/executor.
                ctx.add_error(
                    ErrorCode::InvalidArgument,
                    format!(
                        "Fragment spread '{}' is not allowed at query root; \
                         use it inside a collection selection set instead",
                        fragment_name
                    ),
                );
            }
        }
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

/// Validate an inline fragment
///
/// `expected_collection` is `Some(name)` when the fragment appears inside a
/// collection selection set — the type condition must match the enclosing
/// collection. At root level (query/subscription) it is `None`.
fn validate_inline_fragment<S: SchemaProvider>(
    inline: &ast::InlineFragment,
    expected_collection: Option<&str>,
    ctx: &mut ValidationContext<'_, S>,
) {
    // When nested inside a collection, the type condition must match it
    if let Some(enc) = expected_collection {
        if inline.type_condition != enc {
            ctx.add_error(
                ErrorCode::TypeMismatch,
                format!(
                    "Inline fragment on '{}' cannot appear inside '{}'",
                    inline.type_condition, enc
                ),
            );
            return;
        }
    }

    // Check if type condition refers to a valid collection
    if !ctx.schema.collection_exists(&inline.type_condition) {
        ctx.add_error(
            ErrorCode::UnknownCollection,
            format!(
                "Unknown collection '{}' in inline fragment",
                inline.type_condition
            ),
        );
        return;
    }

    // Validate inner selection set using the type condition as context
    if let Some(collection) = ctx.schema.get_collection(&inline.type_condition) {
        validate_selection_set(&inline.selection_set, collection, ctx);
    }
}

/// Validate a subscription operation
fn validate_subscription<S: SchemaProvider>(
    sub: &Subscription,
    ctx: &mut ValidationContext<'_, S>,
) {
    if let Some(name) = &sub.name {
        ctx.push_path(name);
    }

    // Validate variable definitions
    validate_variable_definitions(&sub.variable_definitions, ctx);

    // Validate selection set
    for selection in &sub.selection_set {
        match selection {
            Selection::Field(field) => {
                validate_field(field, None, ctx);
            }
            Selection::InlineFragment(inline) => {
                validate_inline_fragment(inline, None, ctx);
            }
            Selection::FragmentSpread(fragment_name) => {
                if let Some(fragment) = ctx.fragments.get(fragment_name) {
                    validate_fragment_spread(fragment_name, &fragment.type_condition, ctx);
                } else {
                    ctx.add_error(
                        ErrorCode::UnknownFragment,
                        format!("Fragment '{}' is not defined", fragment_name),
                    );
                }
            }
        }
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
///
/// * `parent_collection` - If Some, this field is within a collection and we check field types.
///                         If None, this is a top-level field that should be a collection reference.
fn validate_field<S: SchemaProvider>(
    field: &Field,
    parent_collection: Option<&Collection>,
    ctx: &mut ValidationContext<'_, S>,
) {
    let field_name = field.alias.as_ref().unwrap_or(&field.name);
    ctx.push_path(field_name);

    match parent_collection {
        None => {
            // Top-level field - always validate collection existence (even for leaf fields)
            let collection_name = &field.name;
            if !ctx.schema.collection_exists(collection_name) {
                ctx.add_error(
                    ErrorCode::UnknownCollection,
                    format!("Collection '{}' does not exist", collection_name),
                );
            } else if field.selection_set.is_empty() {
                ctx.add_error(
                    ErrorCode::InvalidInput,
                    format!("Collection '{}' requires a selection set", collection_name),
                );
            } else if let Some(collection) = ctx.schema.get_collection(collection_name) {
                // Validate nested fields against collection schema
                validate_selection_set(&field.selection_set, collection, ctx);

                // Validate filter if present
                for arg in &field.arguments {
                    if arg.name == "where" || arg.name == "filter" {
                        report_unknown_filter_ops(&arg.value, ctx);
                        if let Some(filter) = extract_filter_from_value(&arg.value) {
                            validate_filter(&filter, collection, ctx);
                        }
                    }
                }
            }
        }
        Some(collection) => {
            // Nested field within a collection - only check selection set if non-empty
            if !field.selection_set.is_empty() {
                if let Some(field_def) = collection.fields.get(&field.name) {
                    match &field_def.field_type {
                        FieldType::Nested(nested_schema) => {
                            // Validate selection set against nested schema
                            validate_nested_selection_set(&field.selection_set, nested_schema, ctx);
                        }
                        FieldType::Object | FieldType::Any => {
                            // Object/Any type - selection set is valid but we can't validate fields
                            // (no schema to validate against - schemaless/dynamic data)
                        }
                        FieldType::Scalar(ScalarType::Object)
                        | FieldType::Scalar(ScalarType::Any) => {
                            // Scalar Object/Any - also allow selection sets without validation
                        }
                        _ => {
                            // Non-object type with selection set - this is an error
                            ctx.add_error(
                                ErrorCode::TypeMismatch,
                                format!(
                                    "Field '{}' is not an object type but has a selection set",
                                    field.name
                                ),
                            );
                        }
                    }
                }
                // If field doesn't exist, the error is already reported by validate_selection_set
            }
        }
    }

    ctx.pop_path();
}

/// Validate selection set against collection schema
fn validate_selection_set<S: SchemaProvider>(
    selections: &[Selection],
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    let mut aliases_seen: HashMap<String, bool> = HashMap::new();

    for selection in selections {
        // Validate based on selection type
        match selection {
            Selection::Field(field) => {
                let display_name = field.alias.as_ref().unwrap_or(&field.name);

                // Check for duplicate aliases
                if aliases_seen.contains_key(display_name) {
                    ctx.add_error(
                        ErrorCode::DuplicateAlias,
                        format!("Duplicate field/alias '{}' in selection", display_name),
                    );
                }
                aliases_seen.insert(display_name.clone(), true);

                // Check if field exists in collection schema
                let field_name = &field.name;
                if !field_name.starts_with("__") && field_name != "id" {
                    if !collection.fields.contains_key(field_name) {
                        ctx.add_error(
                            ErrorCode::UnknownField,
                            format!(
                                "Field '{}' does not exist in collection '{}'",
                                field_name, collection.name
                            ),
                        );
                    }
                }

                // Recursively validate nested selection with parent context
                validate_field(field, Some(collection), ctx);
            }
            Selection::InlineFragment(inline) => {
                validate_inline_fragment(inline, Some(&collection.name), ctx);
            }
            Selection::FragmentSpread(fragment_name) => {
                validate_fragment_spread(fragment_name, &collection.name, ctx);
            }
        }
    }
}

/// Validate selection set against a nested object schema
fn validate_nested_selection_set<S: SchemaProvider>(
    selections: &[Selection],
    nested_schema: &HashMap<String, crate::types::FieldDefinition>,
    ctx: &mut ValidationContext<'_, S>,
) {
    let mut aliases_seen: HashMap<String, bool> = HashMap::new();

    for selection in selections {
        match selection {
            Selection::Field(field) => {
                let display_name = field.alias.as_ref().unwrap_or(&field.name);

                // Check for duplicate aliases
                if aliases_seen.contains_key(display_name) {
                    ctx.add_error(
                        ErrorCode::DuplicateAlias,
                        format!("Duplicate field/alias '{}' in selection", display_name),
                    );
                }
                aliases_seen.insert(display_name.clone(), true);

                // Check if field exists in nested schema
                let field_name = &field.name;
                if !field_name.starts_with("__") {
                    if !nested_schema.contains_key(field_name) {
                        ctx.add_error(
                            ErrorCode::UnknownField,
                            format!("Field '{}' does not exist in nested object", field_name),
                        );
                    } else if !field.selection_set.is_empty() {
                        // Validate further nesting
                        if let Some(field_def) = nested_schema.get(field_name) {
                            match &field_def.field_type {
                                FieldType::Nested(deeper_schema) => {
                                    ctx.push_path(field_name);
                                    validate_nested_selection_set(
                                        &field.selection_set,
                                        deeper_schema,
                                        ctx,
                                    );
                                    ctx.pop_path();
                                }
                                FieldType::Object | FieldType::Any => {
                                    // Object/Any type - allow selection but can't validate
                                }
                                FieldType::Scalar(ScalarType::Object)
                                | FieldType::Scalar(ScalarType::Any) => {
                                    // Scalar Object/Any - allow selection but can't validate
                                }
                                _ => {
                                    ctx.add_error(
                                        ErrorCode::TypeMismatch,
                                        format!(
                                            "Field '{}' is not an object type but has a selection set",
                                            field_name
                                        ),
                                    );
                                }
                            }
                        }
                    }
                }
            }
            Selection::InlineFragment(_) | Selection::FragmentSpread(_) => {
                // Fragments in nested objects not supported for now
                ctx.add_error(
                    ErrorCode::InvalidInput,
                    "Fragments are not supported in nested object selections".to_string(),
                );
            }
        }
    }
}

/// Validate a fragment spread
fn validate_fragment_spread<S: SchemaProvider>(
    fragment_name: &str,
    expected_collection: &str,
    ctx: &mut ValidationContext<'_, S>,
) {
    // 0. Check for cycles
    if ctx.validating_fragments.contains(fragment_name) {
        ctx.add_error(
            ErrorCode::InvalidInput,
            format!("Fragment '{}' contains a cyclic reference", fragment_name),
        );
        return;
    }

    // 1. Check if fragment exists
    let fragment_opt = ctx.fragments.get(fragment_name).cloned();

    if let Some(fragment) = fragment_opt {
        // 2. Check type condition matches
        if fragment.type_condition != expected_collection {
            ctx.add_error(
                ErrorCode::TypeMismatch,
                format!(
                    "Fragment '{}' is defined on '{}' but used on '{}'",
                    fragment_name, fragment.type_condition, expected_collection
                ),
            );
            return;
        }

        // 3. Validate fragment's selection set against the collection
        if let Some(collection) = ctx.schema.get_collection(expected_collection) {
            ctx.push_path(&format!("...{}", fragment_name));
            ctx.validating_fragments.insert(fragment_name.to_string());
            validate_selection_set(&fragment.selection_set, collection, ctx);
            ctx.validating_fragments.remove(fragment_name);
            ctx.pop_path();
        }
    } else {
        ctx.add_error(
            ErrorCode::UnknownFragment,
            format!("Fragment '{}' is not defined", fragment_name),
        );
    }
}

/// Validate a mutation operation
fn validate_mutation_operation<S: SchemaProvider>(
    op: &ast::MutationOperation,
    ctx: &mut ValidationContext<'_, S>,
) {
    match &op.operation {
        ast::MutationOp::Insert { collection, data } => {
            if let Some(col_def) = ctx.schema.get_collection(collection) {
                validate_object_against_schema(data, col_def, ctx);
            } else {
                ctx.add_error(
                    ErrorCode::UnknownCollection,
                    format!("Collection '{}' does not exist", collection),
                );
            }
        }
        ast::MutationOp::InsertMany { collection, data } => {
            if let Some(col_def) = ctx.schema.get_collection(collection) {
                for item in data {
                    validate_object_against_schema(item, col_def, ctx);
                }
            } else {
                ctx.add_error(
                    ErrorCode::UnknownCollection,
                    format!("Collection '{}' does not exist", collection),
                );
            }
        }
        ast::MutationOp::Update {
            collection, data, ..
        }
        | ast::MutationOp::Upsert {
            collection, data, ..
        } => {
            if let Some(col_def) = ctx.schema.get_collection(collection) {
                // Validate partial object for updates (skipping required checks)
                validate_partial_object(data, col_def, ctx);
            } else {
                ctx.add_error(
                    ErrorCode::UnknownCollection,
                    format!("Collection '{}' does not exist", collection),
                );
            }
        }
        ast::MutationOp::Delete { collection, .. } => {
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

/// Validate an object value against a collection schema
fn validate_object_against_schema<S: SchemaProvider>(
    value: &Value,
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    match value {
        Value::Object(map) => {
            // 1. Check provided fields validity
            for (key, val) in map {
                if let Some(field_def) = collection.fields.get(key) {
                    // Check if null is allowed
                    if matches!(val, Value::Null) && !field_def.nullable {
                        ctx.add_error(
                            ErrorCode::InvalidInput,
                            format!("Field '{}' cannot be null", key),
                        );
                    } else if !matches!(val, Value::Null)
                        && !validate_value_against_type(val, &field_def.field_type)
                    {
                        ctx.add_error(
                            ErrorCode::TypeMismatch,
                            format!(
                                "Type mismatch for field '{}': expected {:?}, got {:?}",
                                key,
                                field_def.field_type,
                                value_type_name(val)
                            ),
                        );
                    }
                } else if key != "id" {
                    ctx.add_error(
                        ErrorCode::UnknownField,
                        format!(
                            "Field '{}' not defined in collection '{}'",
                            key, collection.name
                        ),
                    );
                }
            }

            // 2. Check for missing required fields
            for (name, def) in &collection.fields {
                if !def.nullable && !map.contains_key(name) {
                    ctx.add_error(
                        ErrorCode::InvalidInput,
                        format!("Missing required field '{}'", name),
                    );
                }
            }
        }
        _ => {
            ctx.add_error(
                ErrorCode::TypeMismatch,
                format!(
                    "Expected object for collection '{}', got {:?}",
                    collection.name,
                    value_type_name(value)
                ),
            );
        }
    }
}

/// Validate a partial object (field subset) against a collection schema
/// Used for Update operations where missing required fields are allowed
fn validate_partial_object<S: SchemaProvider>(
    value: &Value,
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    match value {
        Value::Object(map) => {
            // Check provided fields validity
            for (key, val) in map {
                if let Some(field_def) = collection.fields.get(key) {
                    // Check if null is allowed
                    if matches!(val, Value::Null) && !field_def.nullable {
                        ctx.add_error(
                            ErrorCode::InvalidInput,
                            format!("Field '{}' cannot be null", key),
                        );
                    } else if !matches!(val, Value::Null)
                        && !validate_value_against_type(val, &field_def.field_type)
                    {
                        ctx.add_error(
                            ErrorCode::TypeMismatch,
                            format!(
                                "Type mismatch for field '{}': expected {:?}, got {:?}",
                                key,
                                field_def.field_type,
                                value_type_name(val)
                            ),
                        );
                    }
                } else if key != "id" {
                    ctx.add_error(
                        ErrorCode::UnknownField,
                        format!(
                            "Field '{}' not defined in collection '{}'",
                            key, collection.name
                        ),
                    );
                }
            }
        }
        _ => {
            ctx.add_error(
                ErrorCode::TypeMismatch,
                format!(
                    "Expected object for collection '{}', got {:?}",
                    collection.name,
                    value_type_name(value)
                ),
            );
        }
    }
}

/// Recursively validate value against type
fn validate_value_against_type(value: &Value, expected: &FieldType) -> bool {
    use crate::types::ScalarType;
    match (expected, value) {
        // Handle Scalar Types
        (FieldType::Scalar(ScalarType::Any), _) => true,
        (_, Value::Null) => true,
        (_, Value::Variable(_)) => true,

        (FieldType::Scalar(ScalarType::String), Value::String(_)) => true,
        (FieldType::Scalar(ScalarType::Int), Value::Int(_)) => true,
        (FieldType::Scalar(ScalarType::Float), Value::Float(_)) => true,
        (FieldType::Scalar(ScalarType::Float), Value::Int(_)) => true,
        (FieldType::Scalar(ScalarType::Bool), Value::Boolean(_)) => true,
        (FieldType::Scalar(ScalarType::Uuid), Value::String(_)) => true,

        // Handle Array Types
        (FieldType::Array(scalar_inner), Value::Array(items)) => {
            // Check each item matches the scalar inner type
            let inner_field_type = FieldType::Scalar(scalar_inner.clone());
            items
                .iter()
                .all(|item| validate_value_against_type(item, &inner_field_type))
        }

        // Handle Objects
        (FieldType::Object, Value::Object(_)) => true,
        // Handle Nested Objects (deep validation)
        (FieldType::Nested(schema), Value::Object(map)) => {
            // 1. Check all provided fields exist in schema and match type
            for (key, val) in map {
                if let Some(def) = schema.get(key) {
                    if matches!(val, Value::Null) {
                        if !def.nullable {
                            return false; // Null not allowed
                        }
                    } else if !validate_value_against_type(val, &def.field_type) {
                        return false; // Type mismatch in nested field
                    }
                } else {
                    return false; // Unknown field in nested object
                }
            }
            // 2. Check all required fields are provided
            for (key, def) in schema.iter() {
                if !def.nullable && !map.contains_key(key) {
                    return false; // Missing required nested field
                }
            }
            true
        }
        (FieldType::Nested(_), _) => false, // Not an object

        // Handle ScalarType::Object (e.g. inside Array<Object>)
        (FieldType::Scalar(ScalarType::Object), Value::Object(_)) => true,
        // Handle ScalarType::Array (e.g. inside Array<Array>)
        (FieldType::Scalar(ScalarType::Array), Value::Array(_)) => true,

        _ => false,
    }
}

/// Validate a filter expression
fn validate_filter<S: SchemaProvider>(
    filter: &Filter,
    collection: &Collection,
    ctx: &mut ValidationContext<'_, S>,
) {
    match filter {
        Filter::Eq(field, value)
        | Filter::Ne(field, value)
        | Filter::Gt(field, value)
        | Filter::Gte(field, value)
        | Filter::Lt(field, value)
        | Filter::Lte(field, value) => {
            validate_filter_field(field, value, collection, ctx);
        }
        Filter::In(field, value) | Filter::NotIn(field, value) => {
            // In/NotIn expects an array value
            if !matches!(value, Value::Array(_)) {
                ctx.add_error(
                    ErrorCode::TypeMismatch,
                    format!("Filter 'in'/'notIn' on '{}' requires an array value", field),
                );
            }
            validate_filter_field_exists(field, collection, ctx);
        }
        Filter::Contains(field, _)
        | Filter::StartsWith(field, _)
        | Filter::EndsWith(field, _)
        | Filter::Matches(field, _) => {
            // String operations - check field is a string type
            if let Some(field_def) = collection.fields.get(field) {
                if field_def.field_type != FieldType::SCALAR_STRING {
                    ctx.add_error(
                        ErrorCode::InvalidFilterOperator,
                        format!("String operator on non-string field '{}'", field),
                    );
                }
            } else {
                validate_filter_field_exists(field, collection, ctx);
            }
        }
        Filter::IsNull(field) | Filter::IsNotNull(field) => {
            validate_filter_field_exists(field, collection, ctx);
        }
        Filter::And(filters) | Filter::Or(filters) => {
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
            format!(
                "Filter field '{}' does not exist in collection '{}'",
                field, collection.name
            ),
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
                    field,
                    field_def.field_type,
                    value_type_name(value)
                ),
            );
        }
    } else {
        ctx.add_error(
            ErrorCode::UnknownField,
            format!(
                "Filter field '{}' does not exist in collection '{}'",
                field, collection.name
            ),
        );
    }
}

/// Check if a value is compatible with a field type
fn is_type_compatible(field_type: &FieldType, value: &Value) -> bool {
    match (field_type, value) {
        (_, Value::Null) => true,        // Null is compatible with any type
        (_, Value::Variable(_)) => true, // Variables are resolved later
        (FieldType::Scalar(ScalarType::String), Value::String(_)) => true,
        (FieldType::Scalar(ScalarType::Int), Value::Int(_)) => true,
        (FieldType::Scalar(ScalarType::Float), Value::Float(_)) => true,
        (FieldType::Scalar(ScalarType::Float), Value::Int(_)) => true, // Int can be used where Float expected
        (FieldType::Scalar(ScalarType::Bool), Value::Boolean(_)) => true,
        (FieldType::Array(_), Value::Array(_)) => true,
        (FieldType::Object, Value::Object(_)) => true,
        (FieldType::Nested(_), Value::Object(_)) => true, // Structural compatibility for filters (deep check might be too expensive/complex here)
        (FieldType::Scalar(ScalarType::Object), Value::Object(_)) => true, // Support for ScalarType::Object
        (FieldType::Scalar(ScalarType::Array), Value::Array(_)) => true, // Support for ScalarType::Array
        (FieldType::Scalar(ScalarType::Any), _) => true, // Any type accepts all values
        (FieldType::Scalar(ScalarType::Uuid), Value::String(_)) => true, // UUIDs are often passed as strings
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

/// Report validation errors for unrecognised filter operators so users get a
/// clear message for typos instead of silently-dropped conditions.
fn report_unknown_filter_ops<S: SchemaProvider>(
    value: &Value,
    ctx: &mut ValidationContext<'_, S>,
) {
    const KNOWN_OPS: &[&str] = &[
        "eq", "ne", "gt", "gte", "lt", "lte",
        "in", "nin", "contains", "startsWith", "endsWith",
        "matches", "isNull", "isNotNull",
    ];
    if let Value::Object(map) = value {
        for (key, val) in map {
            match key.as_str() {
                "and" | "or" => {
                    if let Value::Array(arr) = val {
                        arr.iter().for_each(|v| report_unknown_filter_ops(v, ctx));
                    }
                }
                "not" => report_unknown_filter_ops(val, ctx),
                field => {
                    if let Value::Object(ops) = val {
                        for op in ops.keys() {
                            if !KNOWN_OPS.contains(&op.as_str()) {
                                ctx.add_error(
                                    ErrorCode::InvalidArgument,
                                    format!(
                                        "Unknown filter operator '{}' on field '{}'",
                                        op, field
                                    ),
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Extract a Filter from an AST Value (for parsing where arguments)
fn extract_filter_from_value(value: &Value) -> Option<Filter> {
    match value {
        Value::Object(map) => {
            let mut filters = Vec::new();

            for (key, val) in map {
                match key.as_str() {
                    "and" => {
                        if let Value::Array(arr) = val {
                            let sub_filters: Vec<Filter> =
                                arr.iter().filter_map(extract_filter_from_value).collect();
                            if !sub_filters.is_empty() {
                                filters.push(Filter::And(sub_filters));
                            }
                        }
                    }
                    "or" => {
                        if let Value::Array(arr) = val {
                            let sub_filters: Vec<Filter> =
                                arr.iter().filter_map(extract_filter_from_value).collect();
                            if !sub_filters.is_empty() {
                                filters.push(Filter::Or(sub_filters));
                            }
                        }
                    }
                    "not" => {
                        if let Some(inner) = extract_filter_from_value(val) {
                            filters.push(Filter::Not(Box::new(inner)));
                        }
                    }
                    field => {
                        // Field-level filter: { field: { eq: value } }
                        if let Value::Object(ops) = val {
                            for (op, op_val) in ops {
                                let filter = match op.as_str() {
                                    "eq" => Some(Filter::Eq(field.to_string(), op_val.clone())),
                                    "ne" => Some(Filter::Ne(field.to_string(), op_val.clone())),
                                    "gt" => Some(Filter::Gt(field.to_string(), op_val.clone())),
                                    "gte" => Some(Filter::Gte(field.to_string(), op_val.clone())),
                                    "lt" => Some(Filter::Lt(field.to_string(), op_val.clone())),
                                    "lte" => Some(Filter::Lte(field.to_string(), op_val.clone())),
                                    "in" => Some(Filter::In(field.to_string(), op_val.clone())),
                                    "nin" => Some(Filter::NotIn(field.to_string(), op_val.clone())),
                                    "contains" => {
                                        Some(Filter::Contains(field.to_string(), op_val.clone()))
                                    }
                                    "startsWith" => {
                                        Some(Filter::StartsWith(field.to_string(), op_val.clone()))
                                    }
                                    "endsWith" => {
                                        Some(Filter::EndsWith(field.to_string(), op_val.clone()))
                                    }
                                    "matches" => {
                                        Some(Filter::Matches(field.to_string(), op_val.clone()))
                                    }
                                    "isNull" => Some(Filter::IsNull(field.to_string())),
                                    "isNotNull" => Some(Filter::IsNotNull(field.to_string())),
                                    _ => None,
                                };
                                if let Some(f) = filter {
                                    filters.push(f);
                                }
                            }
                        }
                    }
                }
            }

            match filters.len() {
                0 => None,
                1 => Some(filters.remove(0)),
                _ => Some(Filter::And(filters)),
            }
        }
        _ => None,
    }
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
            ast::Operation::FragmentDefinition(fragment) => {
                // Resolve variables inside fragment field arguments so they are
                // substituted before the fragment is expanded at execution time.
                resolve_in_fields(&mut fragment.selection_set, variables)?;
            }
            ast::Operation::Introspection(_) => {}      // No variables in introspection
            ast::Operation::Handler(_) => {}            // Handlers don't have variable resolution
        }
    }
    Ok(())
}

fn resolve_in_fields(
    fields: &mut [Selection],
    variables: &HashMap<String, ast::Value>,
) -> Result<(), ValidationError> {
    for selection in fields {
        match selection {
            Selection::Field(field) => {
                // Resolve variables in arguments
                for arg in &mut field.arguments {
                    resolve_in_value(&mut arg.value, variables)?;
                }
                // Recursively resolve in nested selection
                resolve_in_fields(&mut field.selection_set, variables)?;
            }
            Selection::InlineFragment(inline) => {
                resolve_in_fields(&mut inline.selection_set, variables)?;
            }
            Selection::FragmentSpread(_) => {
                // Nothing to resolve in fragment spread itself (name)
            }
        }
    }
    Ok(())
}

fn resolve_in_mutation_op(
    op: &mut ast::MutationOperation,
    variables: &HashMap<String, ast::Value>,
) -> Result<(), ValidationError> {
    match &mut op.operation {
        ast::MutationOp::Insert { data, .. }
        | ast::MutationOp::Update { data, .. }
        | ast::MutationOp::Upsert { data, .. } => {
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
    use crate::types::FieldDefinition;

    fn create_test_schema() -> InMemorySchema {
        let mut schema = InMemorySchema::new();

        let mut users_fields = HashMap::new();
        users_fields.insert(
            "name".to_string(),
            FieldDefinition {
                field_type: FieldType::SCALAR_STRING,
                unique: false,
                indexed: false,
                nullable: false,
            },
        );
        users_fields.insert(
            "email".to_string(),
            FieldDefinition {
                field_type: FieldType::SCALAR_STRING,
                unique: true,
                indexed: true,
                nullable: false,
            },
        );
        users_fields.insert(
            "age".to_string(),
            FieldDefinition {
                field_type: FieldType::SCALAR_INT,
                unique: false,
                indexed: false,
                nullable: false,
            },
        );
        users_fields.insert(
            "active".to_string(),
            FieldDefinition {
                field_type: FieldType::SCALAR_BOOL,
                unique: false,
                indexed: false,
                nullable: false,
            },
        );

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
            operations: vec![ast::Operation::Query(Query {
                name: None,
                variable_definitions: vec![],
                directives: vec![],
                selection_set: vec![Selection::Field(Field {
                    alias: None,
                    name: "nonexistent".to_string(),
                    arguments: vec![],
                    directives: vec![],
                    selection_set: vec![Selection::Field(Field {
                        alias: None,
                        name: "id".to_string(),
                        arguments: vec![],
                        directives: vec![],
                        selection_set: vec![],
                    })],
                })],
                variables_values: HashMap::new(),
            })],
        };

        let result = validate_document(&doc, &schema, HashMap::new());
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.code == ErrorCode::UnknownCollection)
        );
    }

    #[test]
    fn test_validate_unknown_field() {
        let schema = create_test_schema();
        let doc = Document {
            operations: vec![ast::Operation::Query(Query {
                name: None,
                variable_definitions: vec![],
                directives: vec![],
                selection_set: vec![Selection::Field(Field {
                    alias: None,
                    name: "users".to_string(),
                    arguments: vec![],
                    directives: vec![],
                    selection_set: vec![Selection::Field(Field {
                        alias: None,
                        name: "nonexistent_field".to_string(),
                        arguments: vec![],
                        directives: vec![],
                        selection_set: vec![],
                    })],
                })],
                variables_values: HashMap::new(),
            })],
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
            operations: vec![ast::Operation::Query(Query {
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
            })],
        };

        // No variables provided
        let result = validate_document(&doc, &schema, HashMap::new());
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.code == ErrorCode::MissingRequiredVariable)
        );
    }

    #[test]
    fn test_validate_valid_query() {
        let schema = create_test_schema();
        let doc = Document {
            operations: vec![ast::Operation::Query(Query {
                name: Some("GetUsers".to_string()),
                variable_definitions: vec![],
                directives: vec![],
                selection_set: vec![Selection::Field(Field {
                    alias: None,
                    name: "users".to_string(),
                    arguments: vec![],
                    directives: vec![],
                    selection_set: vec![
                        Selection::Field(Field {
                            alias: None,
                            name: "id".to_string(),
                            arguments: vec![],
                            directives: vec![],
                            selection_set: vec![],
                        }),
                        Selection::Field(Field {
                            alias: None,
                            name: "name".to_string(),
                            arguments: vec![],
                            directives: vec![],
                            selection_set: vec![],
                        }),
                        Selection::Field(Field {
                            alias: None,
                            name: "email".to_string(),
                            arguments: vec![],
                            directives: vec![],
                            selection_set: vec![],
                        }),
                    ],
                })],
                variables_values: HashMap::new(),
            })],
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
        assert!(
            ctx.errors
                .iter()
                .any(|e| e.code == ErrorCode::InvalidFilterOperator)
        );
    }

    #[test]
    fn test_resolve_variables() {
        let mut doc = Document {
            operations: vec![ast::Operation::Query(Query {
                name: None,
                variable_definitions: vec![],
                directives: vec![],
                selection_set: vec![Selection::Field(Field {
                    alias: None,
                    name: "users".to_string(),
                    arguments: vec![ast::Argument {
                        name: "limit".to_string(),
                        value: Value::Variable("pageSize".to_string()),
                    }],
                    directives: vec![],
                    selection_set: vec![],
                })],
                variables_values: HashMap::new(),
            })],
        };

        let mut vars = HashMap::new();
        vars.insert("pageSize".to_string(), Value::Int(10));

        let result = resolve_variables(&mut doc, &vars);
        assert!(result.is_ok());

        // Check that variable was resolved
        if let ast::Operation::Query(query) = &doc.operations[0] {
            if let Selection::Field(user_field) = &query.selection_set[0] {
                let arg = &user_field.arguments[0];
                assert!(matches!(arg.value, Value::Int(10)));
            } else {
                panic!("Expected Selection::Field");
            }
        } else {
            panic!("Expected Query operation");
        }
    }
}
