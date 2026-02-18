//! Aurora Query Language (AQL) Parser
//!
//! Provides parsing for AQL using Pest grammar.

use pest::Parser;
use pest_derive::Parser;

pub mod ast;
pub mod executor;
pub mod executor_utils;
pub mod validator;

#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct AQLParser;

use crate::error::{AqlError, ErrorCode, Result};
use ast::*;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Reserved keywords that cannot be used as field/collection names
const RESERVED_KEYWORDS: &[&str] = &[
    "query",
    "mutation",
    "subscription",
    "fragment",
    "on",
    "transaction",
    "schema",
    "collection",
    "type",
    "enum",
    "scalar",
    "insertInto",
    "insertMany",
    "update",
    "upsert",
    "deleteFrom",
    "enqueueJob",
    "enqueueJobs",
    "import",
    "export",
    "where",
    "orderBy",
    "limit",
    "offset",
    "first",
    "last",
    "after",
    "before",
    "search",
    "validate",
    "lookup",
    "aggregate",
    "groupBy",
    "windowFunc",
    "downsample",
    "and",
    "or",
    "not",
    "eq",
    "ne",
    "gt",
    "gte",
    "lt",
    "lte",
    "in",
    "nin",
    "contains",
    "containsAny",
    "containsAll",
    "startsWith",
    "endsWith",
    "matches",
    "isNull",
    "isNotNull",
    "near",
    "within",
    "true",
    "false",
    "null",
    "ASC",
    "DESC",
];

/// Generate a helpful error message, with specific detection for reserved keywords
fn get_helpful_error_message(input: &str, line: usize, col: usize) -> String {
    // Try to extract the token at the error location
    if let Some(line_content) = input.lines().nth(line.saturating_sub(1)) {
        // Extract the word at or near the column position
        let chars: Vec<char> = line_content.chars().collect();
        if col > 0 && col <= chars.len() {
            // Find word boundaries around the column
            let start = (0..col.saturating_sub(1))
                .rev()
                .find(|&i| !chars[i].is_alphanumeric() && chars[i] != '_')
                .map(|i| i + 1)
                .unwrap_or(0);

            let end = (col.saturating_sub(1)..chars.len())
                .find(|&i| !chars[i].is_alphanumeric() && chars[i] != '_')
                .unwrap_or(chars.len());

            let word: String = chars[start..end].iter().collect();

            // Check if the word is a reserved keyword
            if RESERVED_KEYWORDS.contains(&word.as_str()) {
                return format!(
                    "Syntax error at line {}, column {}: '{}' is a reserved keyword and cannot be used as a field name. \
                    Consider using a different name like '{}_value' or '{}Type'.",
                    line,
                    col,
                    word,
                    word.to_lowercase(),
                    word.to_lowercase()
                );
            }
        }
    }

    // Default message if no reserved keyword detected
    format!("Syntax error at line {}, column {}", line, col)
}

/// Parse an AQL query string into an AST Document
pub fn parse(input: &str) -> Result<Document> {
    let pairs = AQLParser::parse(Rule::document, input).map_err(|e| {
        let (line, col) = match e.line_col {
            pest::error::LineColLocation::Pos((l, c)) => (l, c),
            pest::error::LineColLocation::Span((l, c), _) => (l, c),
        };

        // Log full error details internally for debugging
        #[cfg(debug_assertions)]
        eprintln!("Parse error details: {}", e);

        // Try to extract the problematic token from the error location
        let user_message = get_helpful_error_message(input, line, col);

        AqlError::new(ErrorCode::ProtocolError, user_message).with_location(line, col)
    })?;

    let mut operations = Vec::new();

    for pair in pairs {
        if pair.as_rule() == Rule::document {
            for inner in pair.into_inner() {
                match inner.as_rule() {
                    Rule::operation => {
                        if let Some(op) = parse_operation(inner)? {
                            operations.push(op);
                        }
                    }
                    Rule::fragment_definition => {
                        operations.push(Operation::FragmentDefinition(parse_fragment_definition(
                            inner,
                        )?));
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(Document { operations })
}

/// Parse an AQL query with variable values
pub fn parse_with_variables(input: &str, variables: JsonValue) -> Result<Document> {
    let mut doc = parse(input)?;

    let vars: HashMap<String, Value> = if let JsonValue::Object(map) = variables {
        map.into_iter()
            .map(|(k, v)| (k, json_to_aql_value(v)))
            .collect()
    } else {
        HashMap::new()
    };

    for op in &mut doc.operations {
        match op {
            Operation::Query(q) => q.variables_values = vars.clone(),
            Operation::Mutation(m) => m.variables_values = vars.clone(),
            Operation::Subscription(s) => s.variables_values = vars.clone(),
            _ => {}
        }
    }

    Ok(doc)
}

fn json_to_aql_value(json: JsonValue) -> Value {
    match json {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Boolean(b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        JsonValue::String(s) => Value::String(s),
        JsonValue::Array(arr) => Value::Array(arr.into_iter().map(json_to_aql_value).collect()),
        JsonValue::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, json_to_aql_value(v)))
                .collect(),
        ),
    }
}

fn parse_operation(pair: pest::iterators::Pair<Rule>) -> Result<Option<Operation>> {
    match pair.as_rule() {
        Rule::operation => {
            for inner in pair.into_inner() {
                return parse_operation(inner);
            }
            Ok(None)
        }
        Rule::query_operation => Ok(Some(Operation::Query(parse_query(pair)?))),
        Rule::mutation_operation => Ok(Some(Operation::Mutation(parse_mutation(pair)?))),
        Rule::subscription_operation => {
            Ok(Some(Operation::Subscription(parse_subscription(pair)?)))
        }
        Rule::schema_operation => Ok(Some(Operation::Schema(parse_schema(pair)?))),
        Rule::migration_operation => Ok(Some(Operation::Migration(parse_migration(pair)?))),
        Rule::introspection_query => Ok(Some(Operation::Introspection(parse_introspection_query(
            pair,
        )?))),
        Rule::EOI => Ok(None),
        _ => Ok(None),
    }
}

fn parse_query(pair: pest::iterators::Pair<Rule>) -> Result<Query> {
    let mut name = None;
    let mut variable_definitions = Vec::new();
    let mut directives = Vec::new();
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = Some(inner.as_str().to_string()),
            Rule::variable_definitions => variable_definitions = parse_variable_definitions(inner)?,
            Rule::directives => directives = parse_directives(inner)?,
            Rule::selection_set => selection_set = parse_selection_set(inner)?,
            _ => {}
        }
    }

    Ok(Query {
        name,
        variable_definitions,
        directives,
        selection_set,
        variables_values: HashMap::new(),
    })
}

fn parse_mutation(pair: pest::iterators::Pair<Rule>) -> Result<Mutation> {
    let mut name = None;
    let mut variable_definitions = Vec::new();
    let mut directives = Vec::new();
    let mut operations = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = Some(inner.as_str().to_string()),
            Rule::variable_definitions => variable_definitions = parse_variable_definitions(inner)?,
            Rule::directives => directives = parse_directives(inner)?,
            Rule::mutation_set => operations = parse_mutation_set(inner)?,
            _ => {}
        }
    }

    Ok(Mutation {
        name,
        variable_definitions,
        directives,
        operations,
        variables_values: HashMap::new(),
    })
}

fn parse_subscription(pair: pest::iterators::Pair<Rule>) -> Result<Subscription> {
    let mut name = None;
    let mut variable_definitions = Vec::new();
    let mut directives = Vec::new();
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = Some(inner.as_str().to_string()),
            Rule::variable_definitions => variable_definitions = parse_variable_definitions(inner)?,
            Rule::directives => directives = parse_directives(inner)?,
            Rule::subscription_set => selection_set = parse_subscription_set(inner)?,
            _ => {}
        }
    }

    Ok(Subscription {
        name,
        variable_definitions,
        directives,
        selection_set,
        variables_values: HashMap::new(),
    })
}

fn parse_schema(pair: pest::iterators::Pair<Rule>) -> Result<Schema> {
    let mut operations = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::schema_definition {
            for rule in inner.into_inner() {
                match rule.as_rule() {
                    Rule::define_collection => operations.push(parse_define_collection(rule)?),
                    Rule::alter_collection => operations.push(parse_alter_collection(rule)?),
                    Rule::drop_collection => operations.push(parse_drop_collection(rule)?),
                    _ => {}
                }
            }
        }
    }
    Ok(Schema { operations })
}

fn parse_define_collection(pair: pest::iterators::Pair<Rule>) -> Result<SchemaOp> {
    let mut name = String::new();
    let mut if_not_exists = false;
    let mut fields = Vec::new();
    let mut directives = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = inner.as_str().to_string(),
            Rule::field_definition => fields.push(parse_field_definition(inner)?),
            Rule::directives => directives = parse_directives(inner)?,
            _ => {
                if inner.as_str() == "if" {
                    // Crude check, relying on grammar structure
                    if_not_exists = true;
                }
            }
        }
    }
    Ok(SchemaOp::DefineCollection {
        name,
        if_not_exists,
        fields,
        directives,
    })
}

fn parse_alter_collection(pair: pest::iterators::Pair<Rule>) -> Result<SchemaOp> {
    let mut name = String::new();
    let mut actions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = inner.as_str().to_string(),
            Rule::alter_action => actions.push(parse_alter_action(inner)?),
            _ => {}
        }
    }
    Ok(SchemaOp::AlterCollection { name, actions })
}

fn parse_alter_action(pair: pest::iterators::Pair<Rule>) -> Result<AlterAction> {
    let input_str = pair.as_str().to_string();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::add_action => {
                for field in inner.into_inner() {
                    if field.as_rule() == Rule::field_definition {
                        return Ok(AlterAction::AddField(parse_field_definition(field)?));
                    }
                }
            }
            Rule::drop_action => {
                for id in inner.into_inner() {
                    if id.as_rule() == Rule::identifier {
                        return Ok(AlterAction::DropField(id.as_str().to_string()));
                    }
                }
            }
            Rule::rename_action => {
                let mut ids = Vec::new();
                for id in inner.into_inner() {
                    if id.as_rule() == Rule::identifier {
                        ids.push(id.as_str().to_string());
                    }
                }
                if ids.len() == 2 {
                    return Ok(AlterAction::RenameField {
                        from: ids[0].clone(),
                        to: ids[1].clone(),
                    });
                }
            }
            Rule::modify_action => {
                for field in inner.into_inner() {
                    if field.as_rule() == Rule::field_definition {
                        return Ok(AlterAction::ModifyField(parse_field_definition(field)?));
                    }
                }
            }
            _ => {}
        }
    }

    Err(AqlError::new(
        ErrorCode::ProtocolError,
        format!("Unknown alter action: {}", input_str),
    ))
}

// Re-implementing correctly below with keyword checking
fn parse_drop_collection(pair: pest::iterators::Pair<Rule>) -> Result<SchemaOp> {
    let mut name = String::new();
    let mut if_exists = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = inner.as_str().to_string(),
            _ => {
                if inner.as_str() == "if" {
                    if_exists = true;
                }
            }
        }
    }
    Ok(SchemaOp::DropCollection { name, if_exists })
}

fn parse_field_definition(pair: pest::iterators::Pair<Rule>) -> Result<FieldDef> {
    let mut name = String::new();
    let mut field_type = TypeAnnotation {
        name: "String".to_string(),
        is_array: false,
        is_required: false,
    };
    let mut directives = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = inner.as_str().to_string(),
            Rule::type_annotation => field_type = parse_type_annotation(inner)?,
            Rule::directives => directives = parse_directives(inner)?,
            _ => {}
        }
    }

    Ok(FieldDef {
        name,
        field_type,
        directives,
    })
}

// MIGRATION PARSING

fn parse_migration(pair: pest::iterators::Pair<Rule>) -> Result<Migration> {
    let mut steps = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::migration_step {
            steps.push(parse_migration_step(inner)?);
        }
    }
    Ok(Migration { steps })
}

fn parse_migration_step(pair: pest::iterators::Pair<Rule>) -> Result<MigrationStep> {
    let mut version = String::new();
    let mut actions = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::migration_version => {
                // migration_version -> string
                if let Some(s_pair) = inner.into_inner().next() {
                    // Strip quotes
                    let s = s_pair.as_str();
                    version = s[1..s.len() - 1].to_string();
                }
            }
            Rule::migration_action => {
                for act in inner.into_inner() {
                    match act.as_rule() {
                        Rule::define_collection => {
                            actions.push(MigrationAction::Schema(parse_define_collection(act)?))
                        }
                        Rule::alter_collection => {
                            actions.push(MigrationAction::Schema(parse_alter_collection(act)?))
                        }
                        Rule::drop_collection => {
                            actions.push(MigrationAction::Schema(parse_drop_collection(act)?))
                        }
                        Rule::data_migration => {
                            actions.push(MigrationAction::DataMigration(parse_data_migration(act)?))
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(MigrationStep { version, actions })
}

fn parse_data_migration(pair: pest::iterators::Pair<Rule>) -> Result<DataMigration> {
    let mut collection = String::new();
    let mut transforms = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => collection = inner.as_str().to_string(),
            Rule::data_transform => transforms.push(parse_data_transform(inner)?),
            _ => {}
        }
    }
    Ok(DataMigration {
        collection,
        transforms,
    })
}

fn parse_data_transform(pair: pest::iterators::Pair<Rule>) -> Result<DataTransform> {
    let mut field = String::new();
    let mut expression = String::new();
    let mut filter = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => field = inner.as_str().to_string(),
            Rule::expression => expression = inner.as_str().to_string(),
            Rule::filter_object => {
                // Parse filter_object to Value then convert to Filter
                let filter_value = parse_filter_object_to_value(inner)?;
                filter = Some(value_to_filter(filter_value).map_err(|e| {
                    AqlError::new(
                        ErrorCode::FilterParseError,
                        format!("Failed to parse filter in data transform: {}", e),
                    )
                })?);
            }
            _ => {}
        }
    }
    Ok(DataTransform {
        field,
        expression,
        filter,
    })
}

/// Parse a filter_object grammar rule to an AST Value
fn parse_filter_object_to_value(pair: pest::iterators::Pair<Rule>) -> Result<Value> {
    let mut map = HashMap::new();

    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::filter_field_list {
            for filter_field in inner.into_inner() {
                if filter_field.as_rule() == Rule::filter_field {
                    parse_filter_field_to_map(filter_field, &mut map)?;
                }
            }
        }
    }

    Ok(Value::Object(map))
}

/// Parse a filter_field into the map
fn parse_filter_field_to_map(
    pair: pest::iterators::Pair<Rule>,
    map: &mut HashMap<String, Value>,
) -> Result<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::logical_operator => {
                parse_logical_operator_to_map(inner, map)?;
            }
            Rule::field_filter | Rule::nested_field_filter => {
                let mut field_name = String::new();
                let mut field_ops = HashMap::new();

                for field_inner in inner.into_inner() {
                    match field_inner.as_rule() {
                        Rule::identifier => field_name = field_inner.as_str().to_string(),
                        Rule::string => {
                            let s = field_inner.as_str();
                            field_name = s[1..s.len() - 1].to_string();
                        }
                        Rule::filter_condition => {
                            parse_filter_condition_to_map(field_inner, &mut field_ops)?;
                        }
                        _ => {}
                    }
                }

                if !field_name.is_empty() {
                    map.insert(field_name, Value::Object(field_ops));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Parse a logical operator (and/or/not) into the map
fn parse_logical_operator_to_map(
    pair: pest::iterators::Pair<Rule>,
    map: &mut HashMap<String, Value>,
) -> Result<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::and_operator => {
                let mut filters = Vec::new();
                for and_inner in inner.into_inner() {
                    if and_inner.as_rule() == Rule::filter_object {
                        filters.push(parse_filter_object_to_value(and_inner)?);
                    }
                }
                map.insert("and".to_string(), Value::Array(filters));
            }
            Rule::or_operator => {
                let mut filters = Vec::new();
                for or_inner in inner.into_inner() {
                    if or_inner.as_rule() == Rule::filter_object {
                        filters.push(parse_filter_object_to_value(or_inner)?);
                    }
                }
                map.insert("or".to_string(), Value::Array(filters));
            }
            Rule::not_operator => {
                for not_inner in inner.into_inner() {
                    if not_inner.as_rule() == Rule::filter_object {
                        map.insert("not".to_string(), parse_filter_object_to_value(not_inner)?);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Parse a filter_condition into the map
fn parse_filter_condition_to_map(
    pair: pest::iterators::Pair<Rule>,
    map: &mut HashMap<String, Value>,
) -> Result<()> {
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::filter_operator_list {
            for op in inner.into_inner() {
                if op.as_rule() == Rule::filter_operator {
                    parse_filter_operator_to_map(op, map)?;
                }
            }
        }
    }
    Ok(())
}

/// Parse a filter_operator into the map
fn parse_filter_operator_to_map(
    pair: pest::iterators::Pair<Rule>,
    map: &mut HashMap<String, Value>,
) -> Result<()> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::eq_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("eq".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::ne_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("ne".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::gt_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("gt".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::gte_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("gte".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::lt_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("lt".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::lte_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("lte".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::in_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::array {
                        map.insert("in".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::nin_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::array {
                        map.insert("nin".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::contains_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("contains".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::starts_with_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("startsWith".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::ends_with_operator => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        map.insert("endsWith".to_string(), parse_value(val)?);
                    }
                }
            }
            Rule::is_null_operator => {
                map.insert("isNull".to_string(), Value::Boolean(true));
            }
            Rule::is_not_null_operator => {
                map.insert("isNotNull".to_string(), Value::Boolean(true));
            }
            _ => {}
        }
    }
    Ok(())
}

fn parse_variable_definitions(
    pair: pest::iterators::Pair<Rule>,
) -> Result<Vec<VariableDefinition>> {
    let mut definitions = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::variable_definition {
            definitions.push(parse_variable_definition(inner)?);
        }
    }
    Ok(definitions)
}

fn parse_variable_definition(pair: pest::iterators::Pair<Rule>) -> Result<VariableDefinition> {
    let mut name = String::new();
    let mut var_type = TypeAnnotation {
        name: "String".to_string(),
        is_array: false,
        is_required: false,
    };
    let mut default_value = None;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::variable => name = inner.as_str().trim_start_matches('$').to_string(),
            Rule::type_annotation => var_type = parse_type_annotation(inner)?,
            Rule::default_value => {
                for val in inner.into_inner() {
                    if val.as_rule() == Rule::value {
                        default_value = Some(parse_value(val)?);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(VariableDefinition {
        name,
        var_type,
        default_value,
    })
}

fn parse_type_annotation(pair: pest::iterators::Pair<Rule>) -> Result<TypeAnnotation> {
    let mut name = String::new();
    let mut is_array = false;
    let mut is_required = false;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::type_name => name = inner.as_str().to_string(),
            Rule::array_type => {
                is_array = true;
                for arr_inner in inner.into_inner() {
                    if arr_inner.as_rule() == Rule::type_annotation {
                        let inner_type = parse_type_annotation(arr_inner)?;
                        name = inner_type.name;
                    }
                }
            }
            Rule::type_modifier => is_required = true,
            _ => name = inner.as_str().to_string(),
        }
    }

    Ok(TypeAnnotation {
        name,
        is_array,
        is_required,
    })
}

fn parse_directives(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Directive>> {
    let mut directives = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::directive {
            directives.push(parse_directive(inner)?);
        }
    }
    Ok(directives)
}

fn parse_directive(pair: pest::iterators::Pair<Rule>) -> Result<Directive> {
    let mut name = String::new();
    let mut arguments = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::directive_name | Rule::identifier => name = inner.as_str().to_string(),
            Rule::arguments => arguments = parse_arguments(inner)?,
            _ => {}
        }
    }

    Ok(Directive { name, arguments })
}

fn parse_selection_set(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Selection>> {
    let mut selections = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::field {
            selections.push(parse_selection(inner)?);
        }
    }
    Ok(selections)
}

fn parse_subscription_set(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Selection>> {
    let mut selections = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::subscription_field {
            selections.push(parse_selection(inner)?);
        }
    }
    Ok(selections)
}

/// Parse a selection (field, fragment spread, or inline fragment)
fn parse_selection(pair: pest::iterators::Pair<Rule>) -> Result<Selection> {
    // The rule is `field`, which can contain fragment_spread, inline_fragment, etc.
    // field = { fragment_spread | inline_fragment | aggregate_with_alias | ... | alias_name? ~ identifier ... }

    // Check first child to determine type if it's a direct alternative
    let inner = pair.clone().into_inner().next().unwrap();

    match inner.as_rule() {
        Rule::fragment_spread => {
            let name = inner.into_inner().next().unwrap().as_str().to_string();
            return Ok(Selection::FragmentSpread(name));
        }
        Rule::inline_fragment => {
            let mut type_condition = String::new();
            let mut selection_set = Vec::new();
            for child in inner.into_inner() {
                match child.as_rule() {
                    Rule::identifier => type_condition = child.as_str().to_string(),
                    Rule::selection_set => selection_set = parse_selection_set(child)?,
                    _ => {}
                }
            }
            return Ok(Selection::InlineFragment(ast::InlineFragment {
                type_condition,
                selection_set,
            }));
        }
        _ => {
            // It's a field (regular or special)
            return Ok(Selection::Field(parse_field_inner(pair)?));
        }
    }
}

/// Helper to parse the field content into a Field struct
fn parse_field_inner(pair: pest::iterators::Pair<Rule>) -> Result<Field> {
    let mut alias = None;
    let mut name = String::new();
    let mut arguments = Vec::new();
    let mut directives = Vec::new();
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::alias_name => {
                for alias_inner in inner.into_inner() {
                    if alias_inner.as_rule() == Rule::identifier {
                        alias = Some(alias_inner.as_str().to_string());
                    }
                }
            }
            Rule::identifier => name = inner.as_str().to_string(),
            Rule::arguments => arguments = parse_arguments(inner)?,
            Rule::directives => directives = parse_directives(inner)?,
            Rule::sub_selection => {
                for sel in inner.into_inner() {
                    if sel.as_rule() == Rule::selection_set {
                        selection_set = parse_selection_set(sel)?;
                    }
                }
            }
            Rule::aggregate_with_alias => {
                name = "aggregate".to_string();
                for sel in inner.into_inner() {
                    match sel.as_rule() {
                        Rule::alias_name => {
                            for alias_inner in sel.into_inner() {
                                if alias_inner.as_rule() == Rule::identifier {
                                    alias = Some(alias_inner.as_str().to_string());
                                }
                            }
                        }
                        Rule::aggregate_field_list => {
                            for agg_field in sel.into_inner() {
                                if agg_field.as_rule() == Rule::aggregate_field {
                                    selection_set
                                        .push(Selection::Field(parse_aggregate_field(agg_field)?));
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
            Rule::special_field_selection => {
                for sel in inner.into_inner() {
                    match sel.as_rule() {
                        Rule::alias_name => {
                            for alias_inner in sel.into_inner() {
                                if alias_inner.as_rule() == Rule::identifier {
                                    alias = Some(alias_inner.as_str().to_string());
                                }
                            }
                        }
                        Rule::group_by_selection => {
                            name = "groupBy".to_string();
                        }
                        Rule::lookup_selection => {
                            name = "lookup".to_string();
                            let lookup = parse_lookup_selection(sel)?;
                            arguments.push(ast::Argument {
                                name: "collection".to_string(),
                                value: ast::Value::String(lookup.collection),
                            });
                            arguments.push(ast::Argument {
                                name: "localField".to_string(),
                                value: ast::Value::String(lookup.local_field),
                            });
                            arguments.push(ast::Argument {
                                name: "foreignField".to_string(),
                                value: ast::Value::String(lookup.foreign_field),
                            });
                            if let Some(filter) = lookup.filter {
                                arguments.push(ast::Argument {
                                    name: "where".to_string(),
                                    value: filter_to_value(&filter),
                                });
                            }
                            selection_set = lookup.selection_set;
                        }
                        Rule::page_info_selection => {
                            name = "pageInfo".to_string();
                        }
                        Rule::edges_selection => {
                            name = "edges".to_string();
                            for edge_inner in sel.into_inner() {
                                if edge_inner.as_rule() == Rule::edge_fields {
                                    for edge_field in edge_inner.into_inner() {
                                        if edge_field.as_rule() == Rule::edge_field {
                                            let edge_str = edge_field.as_str().trim();
                                            if edge_str.starts_with("cursor") {
                                                selection_set.push(Selection::Field(Field {
                                                    alias: None,
                                                    name: "cursor".to_string(),
                                                    arguments: Vec::new(),
                                                    directives: Vec::new(),
                                                    selection_set: Vec::new(),
                                                }));
                                            } else if edge_str.starts_with("node") {
                                                let mut node_selection = Vec::new();
                                                for node_inner in edge_field.into_inner() {
                                                    if node_inner.as_rule() == Rule::selection_set {
                                                        node_selection =
                                                            parse_selection_set(node_inner)?;
                                                    }
                                                }
                                                selection_set.push(Selection::Field(Field {
                                                    alias: None,
                                                    name: "node".to_string(),
                                                    arguments: Vec::new(),
                                                    directives: Vec::new(),
                                                    selection_set: node_selection,
                                                }));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Rule::downsample_selection => {
                            name = "downsample".to_string();
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(Field {
        alias,
        name,
        arguments,
        directives,
        selection_set,
    })
}

/// Parse an aggregate field (count or function like sum(field: "x"))
/// Supports optional aliases like `totalStock: sum(field: "stock")`
fn parse_aggregate_field(pair: pest::iterators::Pair<Rule>) -> Result<Field> {
    let mut name = String::new();
    let mut alias = None;
    let mut arguments = Vec::new();
    let pair_str = pair.as_str().to_string();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::aggregate_field_alias => {
                // Extract alias from aggregate_field_alias (contains identifier ~ ":")
                for alias_inner in inner.into_inner() {
                    if alias_inner.as_rule() == Rule::identifier {
                        alias = Some(alias_inner.as_str().to_string());
                    }
                }
            }
            Rule::aggregate_field_value => {
                // Contains either "count" or aggregate_function
                let inner_str = inner.as_str().to_string();
                for val_inner in inner.into_inner() {
                    match val_inner.as_rule() {
                        Rule::aggregate_function => {
                            // Parse function like sum(field: "age")
                            for fn_inner in val_inner.into_inner() {
                                match fn_inner.as_rule() {
                                    Rule::aggregate_name => {
                                        name = fn_inner.as_str().to_string();
                                    }
                                    Rule::aggregate_args => {
                                        // Parse field: "name" or fields: [...]
                                        let mut arg_name = String::new();
                                        let mut arg_value = Value::Null;

                                        for arg_inner in fn_inner.into_inner() {
                                            match arg_inner.as_rule() {
                                                Rule::string => {
                                                    let s = arg_inner.as_str();
                                                    arg_value = Value::String(
                                                        s[1..s.len() - 1].to_string(),
                                                    );
                                                    if arg_name.is_empty() {
                                                        arg_name = "field".to_string();
                                                    }
                                                }
                                                Rule::array => {
                                                    arg_value = parse_value(arg_inner)?;
                                                    arg_name = "fields".to_string();
                                                }
                                                _ => {
                                                    // Check for "field" or "fields" keyword
                                                    let text = arg_inner.as_str();
                                                    if text == "field" || text == "fields" {
                                                        arg_name = text.to_string();
                                                    }
                                                }
                                            }
                                        }

                                        if !arg_name.is_empty() && !matches!(arg_value, Value::Null)
                                        {
                                            arguments.push(Argument {
                                                name: arg_name,
                                                value: arg_value,
                                            });
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        _ => {
                            // Should be "count" literal
                        }
                    }
                }

                // If no aggregate_function found, check if it's "count" literal
                if name.is_empty() {
                    let text = inner_str.trim();
                    if text == "count" {
                        name = "count".to_string();
                    }
                }
            }
            _ => {}
        }
    }

    // Fallback: If name is still empty but the raw text contains "count"
    if name.is_empty() {
        let text = pair_str.trim();
        if text == "count" || text.ends_with(": count") || text.contains("count") {
            name = "count".to_string();
        }
    }

    Ok(Field {
        alias,
        name,
        arguments,
        directives: vec![],
        selection_set: vec![],
    })
}

fn parse_mutation_set(pair: pest::iterators::Pair<Rule>) -> Result<Vec<MutationOperation>> {
    let mut operations = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::mutation_field {
            operations.push(parse_mutation_field(inner)?);
        }
    }
    Ok(operations)
}

fn parse_mutation_field(pair: pest::iterators::Pair<Rule>) -> Result<MutationOperation> {
    let mut alias = None;
    let mut operation = MutationOp::Insert {
        collection: String::new(),
        data: Value::Null,
    };
    let mut directives = Vec::new();
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::alias_name => {
                for alias_inner in inner.into_inner() {
                    if alias_inner.as_rule() == Rule::identifier {
                        alias = Some(alias_inner.as_str().to_string());
                    }
                }
            }
            Rule::mutation_call => {
                let (op, sel) = parse_mutation_call(inner)?;
                operation = op;
                selection_set = sel;
            }
            Rule::directives => directives = parse_directives(inner)?,
            _ => {}
        }
    }

    Ok(MutationOperation {
        alias,
        operation,
        directives,
        selection_set,
    })
}

fn parse_mutation_call(pair: pest::iterators::Pair<Rule>) -> Result<(MutationOp, Vec<Selection>)> {
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::insert_mutation => return parse_insert_mutation(inner),
            Rule::update_mutation => return parse_update_mutation(inner),
            Rule::delete_mutation => return parse_delete_mutation(inner),
            Rule::enqueue_job_mutation => return parse_enqueue_job_mutation(inner),
            Rule::transaction_block => return parse_transaction_block(inner),
            _ => {}
        }
    }
    Err(AqlError::new(
        ErrorCode::ProtocolError,
        "Unknown mutation type".to_string(),
    ))
}

fn parse_insert_mutation(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(MutationOp, Vec<Selection>)> {
    let mut collection = String::new();
    let mut data = Value::Null;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::insert_args => {
                for arg in parse_arguments_list(inner)? {
                    match arg.name.as_str() {
                        "collection" => {
                            if let Value::String(s) = arg.value {
                                collection = s;
                            }
                        }
                        "data" => data = arg.value,
                        _ => {}
                    }
                }
            }
            Rule::sub_selection => {
                for sel in inner.into_inner() {
                    if sel.as_rule() == Rule::selection_set {
                        selection_set = parse_selection_set(sel)?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok((MutationOp::Insert { collection, data }, selection_set))
}

fn parse_update_mutation(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(MutationOp, Vec<Selection>)> {
    let mut collection = String::new();
    let mut filter = None;
    let mut data = Value::Null;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::update_args => {
                for arg in parse_arguments_list(inner)? {
                    match arg.name.as_str() {
                        "collection" => {
                            if let Value::String(s) = arg.value {
                                collection = s;
                            }
                        }
                        "where" => filter = Some(value_to_filter(arg.value)?),
                        "data" | "set" => data = arg.value,
                        _ => {}
                    }
                }
            }
            Rule::sub_selection => {
                for sel in inner.into_inner() {
                    if sel.as_rule() == Rule::selection_set {
                        selection_set = parse_selection_set(sel)?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok((
        MutationOp::Update {
            collection,
            filter,
            data,
        },
        selection_set,
    ))
}

fn parse_delete_mutation(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(MutationOp, Vec<Selection>)> {
    let mut collection = String::new();
    let mut filter = None;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::delete_args => {
                for arg in parse_arguments_list(inner)? {
                    match arg.name.as_str() {
                        "collection" => {
                            if let Value::String(s) = arg.value {
                                collection = s;
                            }
                        }
                        "where" => filter = Some(value_to_filter(arg.value)?),
                        _ => {}
                    }
                }
            }
            Rule::sub_selection => {
                for sel in inner.into_inner() {
                    if sel.as_rule() == Rule::selection_set {
                        selection_set = parse_selection_set(sel)?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok((MutationOp::Delete { collection, filter }, selection_set))
}

fn parse_enqueue_job_mutation(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(MutationOp, Vec<Selection>)> {
    let mut job_type = String::new();
    let mut payload = Value::Null;
    let mut priority = JobPriority::Normal;
    let mut scheduled_at = None;
    let mut max_retries = None;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::job_args => {
                for arg in parse_arguments_list(inner)? {
                    match arg.name.as_str() {
                        "jobType" => {
                            if let Value::String(s) = arg.value {
                                job_type = s;
                            }
                        }
                        "payload" => payload = arg.value,
                        "priority" => {
                            if let Value::Enum(s) = arg.value {
                                priority = match s.as_str() {
                                    "LOW" => JobPriority::Low,
                                    "HIGH" => JobPriority::High,
                                    "CRITICAL" => JobPriority::Critical,
                                    _ => JobPriority::Normal,
                                };
                            }
                        }
                        "scheduledAt" => {
                            if let Value::String(s) = arg.value {
                                scheduled_at = Some(s);
                            }
                        }
                        "maxRetries" => {
                            if let Value::Int(n) = arg.value {
                                max_retries = Some(n as u32);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Rule::sub_selection => {
                for sel in inner.into_inner() {
                    if sel.as_rule() == Rule::selection_set {
                        selection_set = parse_selection_set(sel)?;
                    }
                }
            }
            _ => {}
        }
    }

    Ok((
        MutationOp::EnqueueJob {
            job_type,
            payload,
            priority,
            scheduled_at,
            max_retries,
        },
        selection_set,
    ))
}

fn parse_transaction_block(
    pair: pest::iterators::Pair<Rule>,
) -> Result<(MutationOp, Vec<Selection>)> {
    let mut operations = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::mutation_set {
            operations = parse_mutation_set(inner)?;
        }
    }
    Ok((MutationOp::Transaction { operations }, Vec::new()))
}

fn parse_arguments(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Argument>> {
    let mut arguments = Vec::new();
    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::argument_list => {
                for arg in inner.into_inner() {
                    if arg.as_rule() == Rule::argument {
                        arguments.push(parse_argument(arg)?);
                    }
                }
            }
            Rule::argument => arguments.push(parse_argument(inner)?),
            _ => {}
        }
    }
    Ok(arguments)
}

fn parse_arguments_list(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Argument>> {
    let mut arguments = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::argument {
            arguments.push(parse_argument(inner)?);
        }
    }
    Ok(arguments)
}

fn parse_argument(pair: pest::iterators::Pair<Rule>) -> Result<Argument> {
    let mut name = String::new();
    let mut value = Value::Null;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = inner.as_str().to_string(),
            Rule::value => value = parse_value(inner)?,
            _ => {}
        }
    }

    Ok(Argument { name, value })
}

fn parse_value(pair: pest::iterators::Pair<Rule>) -> Result<Value> {
    match pair.as_rule() {
        Rule::value => {
            for inner in pair.into_inner() {
                return parse_value(inner);
            }
            Ok(Value::Null)
        }
        Rule::string => {
            let s = pair.as_str();
            let unquoted = if s.starts_with("\"\"\"") {
                &s[3..s.len() - 3]
            } else {
                &s[1..s.len() - 1]
            };
            Ok(Value::String(unquoted.to_string()))
        }
        Rule::number => {
            let s = pair.as_str();
            if s.contains('.') || s.contains('e') || s.contains('E') {
                Ok(Value::Float(s.parse().map_err(|e| {
                    AqlError::new(ErrorCode::ProtocolError, format!("Invalid float: {}", e))
                })?))
            } else {
                Ok(Value::Int(s.parse().map_err(|e| {
                    AqlError::new(ErrorCode::ProtocolError, format!("Invalid integer: {}", e))
                })?))
            }
        }
        Rule::boolean => Ok(Value::Boolean(pair.as_str() == "true")),
        Rule::null => Ok(Value::Null),
        Rule::variable => Ok(Value::Variable(
            pair.as_str().trim_start_matches('$').to_string(),
        )),
        Rule::enum_value => Ok(Value::Enum(pair.as_str().to_string())),
        Rule::array => {
            let mut values = Vec::new();
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::array_value_list {
                    for val in inner.into_inner() {
                        if val.as_rule() == Rule::value {
                            values.push(parse_value(val)?);
                        }
                    }
                }
            }
            Ok(Value::Array(values))
        }
        Rule::object => {
            let mut map = HashMap::new();
            for inner in pair.into_inner() {
                if inner.as_rule() == Rule::object_field_list {
                    for field in inner.into_inner() {
                        if field.as_rule() == Rule::object_field {
                            let (key, val) = parse_object_field(field)?;
                            map.insert(key, val);
                        }
                    }
                }
            }
            Ok(Value::Object(map))
        }
        _ => Ok(Value::Null),
    }
}

fn parse_object_field(pair: pest::iterators::Pair<Rule>) -> Result<(String, Value)> {
    let mut key = String::new();
    let mut value = Value::Null;

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => key = inner.as_str().to_string(),
            Rule::string => {
                let s = inner.as_str();
                key = s[1..s.len() - 1].to_string();
            }
            Rule::value => value = parse_value(inner)?,
            _ => {}
        }
    }

    Ok((key, value))
}

fn value_to_filter(value: Value) -> Result<Filter> {
    match value {
        Value::Object(map) => {
            let mut filters = Vec::new();
            for (key, val) in map {
                match key.as_str() {
                    "and" => {
                        if let Value::Array(arr) = val {
                            let sub: Result<Vec<_>> =
                                arr.into_iter().map(value_to_filter).collect();
                            filters.push(Filter::And(sub?));
                        }
                    }
                    "or" => {
                        if let Value::Array(arr) = val {
                            let sub: Result<Vec<_>> =
                                arr.into_iter().map(value_to_filter).collect();
                            filters.push(Filter::Or(sub?));
                        }
                    }
                    "not" => filters.push(Filter::Not(Box::new(value_to_filter(val)?))),
                    field => {
                        if let Value::Object(ops) = val {
                            for (op, op_val) in ops {
                                let f = match op.as_str() {
                                    "eq" => Filter::Eq(field.to_string(), op_val),
                                    "ne" => Filter::Ne(field.to_string(), op_val),
                                    "gt" => Filter::Gt(field.to_string(), op_val),
                                    "gte" => Filter::Gte(field.to_string(), op_val),
                                    "lt" => Filter::Lt(field.to_string(), op_val),
                                    "lte" => Filter::Lte(field.to_string(), op_val),
                                    "in" => Filter::In(field.to_string(), op_val),
                                    "nin" => Filter::NotIn(field.to_string(), op_val),
                                    "contains" => Filter::Contains(field.to_string(), op_val),
                                    "startsWith" => Filter::StartsWith(field.to_string(), op_val),
                                    "endsWith" => Filter::EndsWith(field.to_string(), op_val),
                                    "isNull" => Filter::IsNull(field.to_string()),
                                    "isNotNull" => Filter::IsNotNull(field.to_string()),
                                    _ => {
                                        return Err(AqlError::new(
                                            ErrorCode::ProtocolError,
                                            format!("Unknown filter operator: {}", op.as_str()),
                                        ));
                                    }
                                };
                                filters.push(f);
                            }
                        }
                    }
                }
            }
            if filters.len() == 1 {
                Ok(filters.remove(0))
            } else {
                Ok(Filter::And(filters))
            }
        }
        _ => Err(AqlError::new(
            ErrorCode::ProtocolError,
            "Filter must be an object".to_string(),
        )),
    }
}

/// Convert Filter back to Value (reverse of value_to_filter)
fn filter_to_value(filter: &Filter) -> Value {
    use std::collections::HashMap;
    match filter {
        Filter::Eq(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("eq".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::Ne(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("ne".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::Gt(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("gt".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::Gte(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("gte".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::Lt(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("lt".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::Lte(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("lte".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::In(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("in".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::NotIn(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("nin".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::Contains(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("contains".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::StartsWith(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("startsWith".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::EndsWith(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("endsWith".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::Matches(field, val) => {
            let mut inner = HashMap::new();
            inner.insert("matches".to_string(), val.clone());
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::IsNull(field) => {
            let mut inner = HashMap::new();
            inner.insert("isNull".to_string(), Value::Boolean(true));
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::IsNotNull(field) => {
            let mut inner = HashMap::new();
            inner.insert("isNotNull".to_string(), Value::Boolean(true));
            let mut outer = HashMap::new();
            outer.insert(field.clone(), Value::Object(inner));
            Value::Object(outer)
        }
        Filter::And(filters) => {
            let arr: Vec<Value> = filters.iter().map(filter_to_value).collect();
            let mut map = HashMap::new();
            map.insert("and".to_string(), Value::Array(arr));
            Value::Object(map)
        }
        Filter::Or(filters) => {
            let arr: Vec<Value> = filters.iter().map(filter_to_value).collect();
            let mut map = HashMap::new();
            map.insert("or".to_string(), Value::Array(arr));
            Value::Object(map)
        }
        Filter::Not(inner) => {
            let mut map = HashMap::new();
            map.insert("not".to_string(), filter_to_value(inner));
            Value::Object(map)
        }
    }
}

// FRAGMENT AND INTROSPECTION PARSING
fn parse_fragment_definition(pair: pest::iterators::Pair<Rule>) -> Result<ast::FragmentDef> {
    let mut name = String::new();
    let mut type_condition = String::new();
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => {
                if name.is_empty() {
                    name = inner.as_str().to_string();
                } else {
                    type_condition = inner.as_str().to_string();
                }
            }
            Rule::selection_set => {
                selection_set = parse_selection_set(inner)?;
            }
            _ => {}
        }
    }

    Ok(ast::FragmentDef {
        name,
        type_condition,
        selection_set,
    })
}

fn parse_introspection_query(pair: pest::iterators::Pair<Rule>) -> Result<ast::IntrospectionQuery> {
    let mut arguments = Vec::new();
    let mut fields = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::arguments => {
                arguments = parse_arguments(inner)?;
            }
            Rule::introspection_fields => {
                for field in inner.into_inner() {
                    if field.as_rule() == Rule::introspection_field {
                        fields.push(field.as_str().to_string());
                    }
                }
            }
            _ => {}
        }
    }

    Ok(ast::IntrospectionQuery { arguments, fields })
}

fn parse_lookup_selection(pair: pest::iterators::Pair<Rule>) -> Result<ast::LookupSelection> {
    let mut collection = String::new();
    let mut local_field = String::new();
    let mut foreign_field = String::new();
    let mut filter = None;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::lookup_args => {
                for arg in inner.into_inner() {
                    match arg.as_rule() {
                        Rule::string => {
                            let s = arg.as_str();
                            let unquoted = &s[1..s.len() - 1];
                            if collection.is_empty() {
                                collection = unquoted.to_string();
                            } else if local_field.is_empty() {
                                local_field = unquoted.to_string();
                            } else if foreign_field.is_empty() {
                                foreign_field = unquoted.to_string();
                            }
                        }
                        Rule::filter_object => {
                            if let Ok(filter_value) = parse_filter_object_to_value(arg) {
                                filter = Some(value_to_filter(filter_value)?);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Rule::sub_selection => {
                for sel in inner.into_inner() {
                    if sel.as_rule() == Rule::selection_set {
                        let fields = parse_selection_set(sel)?;
                        selection_set = fields;
                    }
                }
            }
            _ => {}
        }
    }

    if collection.is_empty() || local_field.is_empty() || foreign_field.is_empty() {
        return Err(AqlError::new(
            ErrorCode::ProtocolError,
            "Lookup must specify collection, localField, and foreignField".to_string(),
        ));
    }

    Ok(ast::LookupSelection {
        collection,
        local_field,
        foreign_field,
        filter,
        selection_set,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_query() {
        let query = r#"query { users { id name email } }"#;
        let result = parse(query);
        assert!(result.is_ok());
        let doc = result.unwrap();
        assert_eq!(doc.operations.len(), 1);
    }

    #[test]
    fn test_parse_query_with_filter() {
        let query =
            r#"query GetActiveUsers { users(where: { active: { eq: true } }) { id name } }"#;
        let result = parse(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_mutation() {
        let query =
            r#"mutation { insertInto(collection: "users", data: { name: "John" }) { id } }"#;
        let result = parse(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_alter_collection() {
        let schema = r#"
            schema {
                 alter collection users {
                     add age: Int
                     drop legacy_field
                     rename name to full_name
                     modify active: Boolean
                 }
            }
        "#;
        let result = parse(schema);
        assert!(
            result.is_ok(),
            "Failed to parse alter collection: {:?}",
            result.err()
        );

        let doc = result.unwrap();
        if let Operation::Schema(Schema { operations }) = &doc.operations[0] {
            if let SchemaOp::AlterCollection { name, actions } = &operations[0] {
                assert_eq!(name, "users");
                assert_eq!(actions.len(), 4);

                // Check add action
                match &actions[0] {
                    AlterAction::AddField(field) => {
                        assert_eq!(field.name, "age");
                        assert_eq!(field.field_type.name, "Int");
                    }
                    _ => panic!("Expected AddField"),
                }

                // Check drop action
                match &actions[1] {
                    AlterAction::DropField(name) => assert_eq!(name, "legacy_field"),
                    _ => panic!("Expected DropField"),
                }

                // Check rename action
                match &actions[2] {
                    AlterAction::RenameField { from, to } => {
                        assert_eq!(from, "name");
                        assert_eq!(to, "full_name");
                    }
                    _ => panic!("Expected RenameField"),
                }

                // Check modify action
                match &actions[3] {
                    AlterAction::ModifyField(field) => {
                        assert_eq!(field.name, "active");
                        assert_eq!(field.field_type.name, "Boolean");
                    }
                    _ => panic!("Expected ModifyField"),
                }
            } else {
                panic!("Expected AlterCollection operation");
            }
        } else {
            panic!("Expected Schema operation");
        }
    }

    #[test]
    fn test_parse_fragment_definition() {
        let query = r#"
            fragment UserFields on User {
                id
                name
                email
            }
        "#;
        let result = parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse fragment: {:?}",
            result.err()
        );
        let doc = result.unwrap();
        assert_eq!(doc.operations.len(), 1);

        if let Operation::FragmentDefinition(frag) = &doc.operations[0] {
            assert_eq!(frag.name, "UserFields");
            assert_eq!(frag.type_condition, "User");
            assert_eq!(frag.selection_set.len(), 3);
        } else {
            panic!("Expected FragmentDefinition");
        }
    }

    #[test]
    fn test_parse_introspection() {
        let query = r#"__schema { collections, fields }"#;
        let result = parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse introspection: {:?}",
            result.err()
        );
        let doc = result.unwrap();
        assert_eq!(doc.operations.len(), 1);

        if let Operation::Introspection(intro) = &doc.operations[0] {
            assert_eq!(intro.fields.len(), 2);
        } else {
            panic!("Expected Introspection");
        }
    }

    #[test]
    fn test_parse_fragment_with_query() {
        let query = r#"
            fragment UserFields on User {
                id
                name
            }
            
            query GetUsers {
                users {
                    ...UserFields
                }
            }
        "#;
        let result = parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse fragment with query: {:?}",
            result.err()
        );
        let doc = result.unwrap();
        assert_eq!(doc.operations.len(), 2);
    }

    #[test]
    fn test_parse_aggregate_with_alias() {
        let query = r#"
            query {
                products {
                    stats: aggregate {
                        totalStock: sum(field: "stock")
                        avgPrice: avg(field: "price")
                        count
                    }
                }
            }
        "#;
        let result = parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse aggregate: {:?}",
            result.err()
        );

        let doc = result.unwrap();
        if let Operation::Query(q) = &doc.operations[0] {
            if let Selection::Field(products_field) = &q.selection_set[0] {
                assert_eq!(products_field.name, "products");

                // Check inner selection set for aggregate
                if let Selection::Field(agg_field) = &products_field.selection_set[0] {
                    assert_eq!(agg_field.alias, Some("stats".to_string()));
                    assert_eq!(agg_field.name, "aggregate");

                    // Check aggregate selection set
                    assert_eq!(agg_field.selection_set.len(), 3, "Expected 3 agg functions");

                    // Check first agg function
                    if let Selection::Field(total_stock) = &agg_field.selection_set[0] {
                        assert_eq!(
                            total_stock.alias,
                            Some("totalStock".to_string()),
                            "Expected totalStock alias"
                        );
                        assert_eq!(total_stock.name, "sum");
                        assert_eq!(total_stock.arguments.len(), 1);
                        assert_eq!(total_stock.arguments[0].name, "field");
                    } else {
                        panic!("Expected Field for total_stock");
                    }

                    // Check count
                    if let Selection::Field(count) = &agg_field.selection_set[2] {
                        assert_eq!(count.name, "count");
                    } else {
                        panic!("Expected Field for count");
                    }
                } else {
                    panic!("Expected Field for aggregate");
                }
            } else {
                panic!("Expected Field for products");
            }
        } else {
            panic!("Expected Query operation");
        }
    }

    #[test]
    fn test_parse_lookup_selection() {
        let query = r#"
            query {
                orders {
                    id
                    total
                    lookup(collection: "users", localField: "user_id", foreignField: "id") {
                        name
                        email
                    }
                }
            }
        "#;
        let result = parse(query);
        assert!(result.is_ok(), "Failed to parse lookup: {:?}", result.err());

        let doc = result.unwrap();
        if let Operation::Query(q) = &doc.operations[0] {
            if let Selection::Field(orders_field) = &q.selection_set[0] {
                assert_eq!(orders_field.name, "orders");

                // Find the lookup selection
                let lookup_selection = orders_field
                    .selection_set
                    .iter()
                    .find(|f| match f {
                        Selection::Field(field) => field.name == "lookup",
                        _ => false,
                    })
                    .expect("Should have lookup field");

                if let Selection::Field(lookup_field) = lookup_selection {
                    // Check lookup arguments were parsed
                    assert!(
                        lookup_field
                            .arguments
                            .iter()
                            .any(|a| a.name == "collection"),
                        "Should have collection argument"
                    );
                    assert!(
                        lookup_field
                            .arguments
                            .iter()
                            .any(|a| a.name == "localField"),
                        "Should have localField argument"
                    );
                    assert!(
                        lookup_field
                            .arguments
                            .iter()
                            .any(|a| a.name == "foreignField"),
                        "Should have foreignField argument"
                    );

                    // Check collection value
                    let collection_arg = lookup_field
                        .arguments
                        .iter()
                        .find(|a| a.name == "collection")
                        .unwrap();
                    if let ast::Value::String(val) = &collection_arg.value {
                        assert_eq!(val, "users");
                    } else {
                        panic!("collection should be a string");
                    }

                    // Check selection set
                    assert_eq!(lookup_field.selection_set.len(), 2);
                    if let Selection::Field(f) = &lookup_field.selection_set[0] {
                        assert_eq!(f.name, "name");
                    }
                    if let Selection::Field(f) = &lookup_field.selection_set[1] {
                        assert_eq!(f.name, "email");
                    }
                } else {
                    panic!("Lookup should be a Field");
                }
            } else {
                panic!("Expected Field for orders");
            }
        } else {
            panic!("Expected Query operation");
        }
    }

    #[test]
    fn test_parse_lookup_with_filter() {
        let query = r#"
            query {
                orders {
                    id
                    lookup(collection: "users", localField: "user_id", foreignField: "id", where: { active: { eq: true } }) {
                        name
                    }
                }
            }
        "#;
        let result = parse(query);
        assert!(
            result.is_ok(),
            "Failed to parse lookup with filter: {:?}",
            result.err()
        );

        let doc = result.unwrap();
        if let Operation::Query(q) = &doc.operations[0] {
            if let Selection::Field(orders_field) = &q.selection_set[0] {
                let lookup_selection = orders_field
                    .selection_set
                    .iter()
                    .find(|f| match f {
                        Selection::Field(field) => field.name == "lookup",
                        _ => false,
                    })
                    .expect("Should have lookup field");

                if let Selection::Field(lookup_field) = lookup_selection {
                    // Check where argument exists
                    assert!(
                        lookup_field.arguments.iter().any(|a| a.name == "where"),
                        "Should have where argument for filter"
                    );
                } else {
                    panic!("Expected Field for lookup");
                }
            } else {
                panic!("Expected Field for orders");
            }
        } else {
            panic!("Expected Query operation");
        }
    }
}
