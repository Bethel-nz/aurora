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
    FragmentDefinition(FragmentDef),
    Introspection(IntrospectionQuery),
    Handler(HandlerDef),
}

/// Query operation
#[derive(Debug, Clone)]
pub struct Query {
    pub name: Option<String>,
    pub variable_definitions: Vec<VariableDefinition>,
    pub directives: Vec<Directive>,
    pub selection_set: Vec<Selection>,
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
    pub selection_set: Vec<Selection>,
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

/// Handler definition for event-driven automation
#[derive(Debug, Clone)]
pub struct HandlerDef {
    pub name: String,
    pub trigger: HandlerTrigger,
    pub action: MutationOperation,
}

/// Event trigger for handlers
#[derive(Debug, Clone, PartialEq)]
pub enum HandlerTrigger {
    /// Fires when a background job completes
    JobCompleted,
    /// Fires when a background job fails
    JobFailed,
    /// Fires when a document is inserted (optionally into specific collection)
    Insert { collection: Option<String> },
    /// Fires when a document is updated (optionally in specific collection)
    Update { collection: Option<String> },
    /// Fires when a document is deleted (optionally from specific collection)
    Delete { collection: Option<String> },
    /// Custom event name
    Custom(String),
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
    pub selection_set: Vec<Selection>,
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
    pub selection_set: Vec<Selection>,
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

/// Fragment definition (top-level)
#[derive(Debug, Clone)]
pub struct FragmentDef {
    pub name: String,
    pub type_condition: String,
    pub selection_set: Vec<Selection>,
}

/// Introspection query (__schema)
#[derive(Debug, Clone)]
pub struct IntrospectionQuery {
    pub arguments: Vec<Argument>,
    pub fields: Vec<String>,
}

/// Selection within a selection set
#[derive(Debug, Clone)]
pub enum Selection {
    Field(Field),
    FragmentSpread(String),
    InlineFragment(InlineFragment),
    ComputedField(ComputedField),
    SpecialSelection(SpecialSelection),
}

/// Inline fragment (... on Type { ... })
#[derive(Debug, Clone)]
pub struct InlineFragment {
    pub type_condition: String,
    pub selection_set: Vec<Selection>,
}

/// Computed field with expression
#[derive(Debug, Clone)]
pub struct ComputedField {
    pub alias: String,
    pub expression: ComputedExpression,
}

/// Computed expression types
#[derive(Debug, Clone)]
pub enum ComputedExpression {
    TemplateString(String),
    FunctionCall {
        name: String,
        args: Vec<Expression>,
    },
    PipeExpression {
        base: Box<Expression>,
        operations: Vec<PipeOp>,
    },
    SqlExpression(String),
    AggregateFunction {
        name: String,
        field: String,
    },
}

/// Pipe operation (for pipe expressions)
#[derive(Debug, Clone)]
pub struct PipeOp {
    pub function: String,
    pub args: Vec<Expression>,
}

/// Expression (for computed fields and filters)
#[derive(Debug, Clone)]
pub enum Expression {
    Literal(Value),
    FieldAccess(Vec<String>),
    Variable(String),
    FunctionCall {
        name: String,
        args: Vec<Expression>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
    Ternary {
        condition: Box<Expression>,
        then_expr: Box<Expression>,
        else_expr: Box<Expression>,
    },
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Neg,
}

/// Special selection types
#[derive(Debug, Clone)]
pub enum SpecialSelection {
    Aggregate(AggregateSelection),
    GroupBy(GroupBySelection),
    Lookup(LookupSelection),
    PageInfo(PageInfoSelection),
    Edges(EdgesSelection),
    Downsample(DownsampleSelection),
    WindowFunction(WindowFunctionSelection),
}

/// Aggregate selection
#[derive(Debug, Clone)]
pub struct AggregateSelection {
    pub fields: Vec<AggregateField>,
}

/// Aggregate field (count, sum, avg, etc.)
#[derive(Debug, Clone)]
pub struct AggregateField {
    pub function: String,
    pub field: Option<String>,
}

/// Group by selection
#[derive(Debug, Clone)]
pub struct GroupBySelection {
    pub field: Option<String>,
    pub fields: Option<Vec<String>>,
    pub interval: Option<String>,
    pub result_fields: Vec<String>,
}

/// Lookup selection (manual join)
#[derive(Debug, Clone)]
pub struct LookupSelection {
    pub collection: String,
    pub local_field: String,
    pub foreign_field: String,
    pub filter: Option<Filter>,
    pub selection_set: Vec<Selection>,
}

/// Page info selection
#[derive(Debug, Clone)]
pub struct PageInfoSelection {
    pub fields: Vec<String>,
}

/// Edges selection (cursor pagination)
#[derive(Debug, Clone)]
pub struct EdgesSelection {
    pub fields: Vec<EdgeField>,
}

/// Edge field types
#[derive(Debug, Clone)]
pub enum EdgeField {
    Cursor,
    Node(Vec<Selection>),
}

/// Downsample selection (time-series)
#[derive(Debug, Clone)]
pub struct DownsampleSelection {
    pub interval: String,
    pub aggregation: String,
    pub selection_set: Vec<Selection>,
}

/// Window function selection (time-series)
#[derive(Debug, Clone)]
pub struct WindowFunctionSelection {
    pub alias: String,
    pub field: String,
    pub function: String,
    pub window_size: i64,
}

// ============================================================================
// SPECIAL ARGUMENT TYPES
// ============================================================================

/// Where clause
#[derive(Debug, Clone)]
pub struct WhereClause {
    pub filter: Filter,
}

/// Order by clause
#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub orderings: Vec<Ordering>,
}

/// Single ordering
#[derive(Debug, Clone)]
pub struct Ordering {
    pub field: String,
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

/// Search arguments
#[derive(Debug, Clone)]
pub struct SearchArgs {
    pub query: String,
    pub fields: Vec<String>,
    pub fuzzy: bool,
    pub min_score: Option<f64>,
}

/// Validation arguments
#[derive(Debug, Clone)]
pub struct ValidateArgs {
    pub rules: Vec<ValidationRule>,
}

/// Validation rule for a field
#[derive(Debug, Clone)]
pub struct ValidationRule {
    pub field: String,
    pub constraints: Vec<ValidationConstraint>,
}

/// Validation constraint types
#[derive(Debug, Clone)]
pub enum ValidationConstraint {
    Format(String),
    Min(f64),
    Max(f64),
    MinLength(i64),
    MaxLength(i64),
    Pattern(String),
}
