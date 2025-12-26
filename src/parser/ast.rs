//! AST types for Aurora Query Language (AQL)

use std::collections::HashMap;

/// Root document containing all operations
#[derive(Debug, Clone)]
pub struct Document {
    pub operations: Vec<Operation>,
}

/// Top-level operation types
#[derive(Debug, Clone)]
pub enum Operation {
    Query(Query),
    Mutation(Mutation),
    Subscription(Subscription),
    Schema(Schema),
    Migration(Migration),
}

/// Query operation
#[derive(Debug, Clone)]
pub struct Query {
    pub name: Option<String>,
    pub variable_definitions: Vec<VariableDefinition>,
    pub directives: Vec<Directive>,
    pub selection_set: Vec<Field>,
    pub variables_values: HashMap<String, Value>,
}

/// Mutation operation
#[derive(Debug, Clone)]
pub struct Mutation {
    pub name: Option<String>,
    pub variable_definitions: Vec<VariableDefinition>,
    pub directives: Vec<Directive>,
    pub operations: Vec<MutationOperation>,
    pub variables_values: HashMap<String, Value>,
}

/// Subscription operation
#[derive(Debug, Clone)]
pub struct Subscription {
    pub name: Option<String>,
    pub variable_definitions: Vec<VariableDefinition>,
    pub directives: Vec<Directive>,
    pub selection_set: Vec<Field>,
    pub variables_values: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct Schema {
    pub operations: Vec<SchemaOp>,
}

#[derive(Debug, Clone)]
pub enum SchemaOp {
    DefineCollection {
        name: String,
        if_not_exists: bool,
        fields: Vec<FieldDef>,
        directives: Vec<Directive>,
    },
    AlterCollection {
        name: String,
        actions: Vec<AlterAction>,
    },
    DropCollection {
        name: String,
        if_exists: bool,
    },
}

#[derive(Debug, Clone)]
pub enum AlterAction {
    AddField(FieldDef),
    DropField(String),
    RenameField { from: String, to: String },
    ModifyField(FieldDef),
}

#[derive(Debug, Clone)]
pub struct Migration {
    pub steps: Vec<MigrationStep>,
}

#[derive(Debug, Clone)]
pub struct MigrationStep {
    pub version: String,
    pub actions: Vec<MigrationAction>,
}

#[derive(Debug, Clone)]
pub enum MigrationAction {
    Schema(SchemaOp),
    DataMigration(DataMigration),
}

#[derive(Debug, Clone)]
pub struct DataMigration {
    pub collection: String,
    pub transforms: Vec<DataTransform>,
}

#[derive(Debug, Clone)]
pub struct DataTransform {
    pub field: String,
    pub expression: String, // Rhai expression
    pub filter: Option<Filter>,
}

/// Field definition in schema
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name: String,
    pub field_type: TypeAnnotation,
    pub directives: Vec<Directive>,
}

/// Variable definition
#[derive(Debug, Clone)]
pub struct VariableDefinition {
    pub name: String,
    pub var_type: TypeAnnotation,
    pub default_value: Option<Value>,
}

/// Type annotation
#[derive(Debug, Clone)]
pub struct TypeAnnotation {
    pub name: String,
    pub is_array: bool,
    pub is_required: bool,
}

/// Directive (e.g., @include, @skip)
#[derive(Debug, Clone)]
pub struct Directive {
    pub name: String,
    pub arguments: Vec<Argument>,
}

/// Field selection
#[derive(Debug, Clone)]
pub struct Field {
    pub alias: Option<String>,
    pub name: String,
    pub arguments: Vec<Argument>,
    pub directives: Vec<Directive>,
    pub selection_set: Vec<Field>,
}

/// Argument (key-value pair)
#[derive(Debug, Clone)]
pub struct Argument {
    pub name: String,
    pub value: Value,
}

/// Mutation operation wrapper
#[derive(Debug, Clone)]
pub struct MutationOperation {
    pub alias: Option<String>,
    pub operation: MutationOp,
    pub directives: Vec<Directive>,
    pub selection_set: Vec<Field>,
}

/// Mutation operation types
#[derive(Debug, Clone)]
pub enum MutationOp {
    Insert {
        collection: String,
        data: Value,
    },
    InsertMany {
        collection: String,
        data: Vec<Value>,
    },
    Update {
        collection: String,
        filter: Option<Filter>,
        data: Value,
    },
    Upsert {
        collection: String,
        filter: Option<Filter>,
        data: Value,
    },
    Delete {
        collection: String,
        filter: Option<Filter>,
    },
    EnqueueJob {
        job_type: String,
        payload: Value,
        priority: JobPriority,
        scheduled_at: Option<String>,
        max_retries: Option<u32>,
    },
    Transaction {
        operations: Vec<MutationOperation>,
    },
}

/// Job priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPriority {
    Low,
    Normal,
    High,
    Critical,
}

/// Filter expressions
#[derive(Debug, Clone)]
pub enum Filter {
    Eq(String, Value),
    Ne(String, Value),
    Gt(String, Value),
    Gte(String, Value),
    Lt(String, Value),
    Lte(String, Value),
    In(String, Value),
    NotIn(String, Value),
    Contains(String, Value),
    StartsWith(String, Value),
    EndsWith(String, Value),
    Matches(String, Value),
    IsNull(String),
    IsNotNull(String),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

/// Value types
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Int(i64),
    Float(f64),
    String(String),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    Variable(String),
    Enum(String),
}
