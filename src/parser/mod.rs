//! Aurora Query Language (AQL) Parser
//!
//! Provides parsing for AQL using Pest grammar.

use pest::Parser;
use pest_derive::Parser;

pub mod ast;

#[derive(Parser)]
#[grammar = "parser/grammar.pest"]
pub struct AQLParser;

use crate::error::{AuroraError, Result};
use ast::*;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Parse an AQL query string into an AST Document
pub fn parse(input: &str) -> Result<Document> {
    let pairs = AQLParser::parse(Rule::document, input)
        .map_err(|e| AuroraError::Protocol(format!("Parse error: {}", e)))?;

    let mut operations = Vec::new();

    for pair in pairs {
        if pair.as_rule() == Rule::document {
            for inner in pair.into_inner() {
                if let Some(op) = parse_operation(inner)? {
                    operations.push(op);
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

    if let Some(op) = doc.operations.first_mut() {
        match op {
            Operation::Query(q) => q.variables_values = vars,
            Operation::Mutation(m) => m.variables_values = vars,
            Operation::Subscription(s) => s.variables_values = vars,
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
        JsonValue::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, json_to_aql_value(v))).collect())
        }
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
        Rule::subscription_operation => Ok(Some(Operation::Subscription(parse_subscription(pair)?))),
        Rule::schema_definition => Ok(Some(Operation::Schema(parse_schema(pair)?))),
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
    let mut collections = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::collection_definition {
            collections.push(parse_collection_definition(inner)?);
        }
    }
    Ok(Schema { collections })
}

fn parse_collection_definition(pair: pest::iterators::Pair<Rule>) -> Result<CollectionDef> {
    let mut name = String::new();
    let mut fields = Vec::new();
    let mut directives = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = inner.as_str().to_string(),
            Rule::field_definition => fields.push(parse_field_definition(inner)?),
            Rule::directives => directives = parse_directives(inner)?,
            _ => {}
        }
    }

    Ok(CollectionDef { name, fields, directives })
}

fn parse_field_definition(pair: pest::iterators::Pair<Rule>) -> Result<FieldDef> {
    let mut name = String::new();
    let mut field_type = TypeAnnotation { name: "String".to_string(), is_array: false, is_required: false };
    let mut directives = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::identifier => name = inner.as_str().to_string(),
            Rule::type_annotation => field_type = parse_type_annotation(inner)?,
            Rule::directives => directives = parse_directives(inner)?,
            _ => {}
        }
    }

    Ok(FieldDef { name, field_type, directives })
}

fn parse_variable_definitions(pair: pest::iterators::Pair<Rule>) -> Result<Vec<VariableDefinition>> {
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
    let mut var_type = TypeAnnotation { name: "String".to_string(), is_array: false, is_required: false };
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

    Ok(VariableDefinition { name, var_type, default_value })
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

    Ok(TypeAnnotation { name, is_array, is_required })
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

fn parse_selection_set(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Field>> {
    let mut fields = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::field {
            fields.push(parse_field(inner)?);
        }
    }
    Ok(fields)
}

fn parse_subscription_set(pair: pest::iterators::Pair<Rule>) -> Result<Vec<Field>> {
    let mut fields = Vec::new();
    for inner in pair.into_inner() {
        if inner.as_rule() == Rule::subscription_field {
            fields.push(parse_field(inner)?);
        }
    }
    Ok(fields)
}

fn parse_field(pair: pest::iterators::Pair<Rule>) -> Result<Field> {
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
            Rule::fragment_spread => {
                name = format!("...{}", inner.as_str().trim_start_matches("...").trim());
            }
            _ => {}
        }
    }

    Ok(Field { alias, name, arguments, directives, selection_set })
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
    let mut operation = MutationOp::Insert { collection: String::new(), data: Value::Null };
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

    Ok(MutationOperation { alias, operation, directives, selection_set })
}

fn parse_mutation_call(pair: pest::iterators::Pair<Rule>) -> Result<(MutationOp, Vec<Field>)> {
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
    Err(AuroraError::Protocol("Unknown mutation type".to_string()))
}

fn parse_insert_mutation(pair: pest::iterators::Pair<Rule>) -> Result<(MutationOp, Vec<Field>)> {
    let mut collection = String::new();
    let mut data = Value::Null;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::insert_args => {
                for arg in parse_arguments_list(inner)? {
                    match arg.name.as_str() {
                        "collection" => if let Value::String(s) = arg.value { collection = s; }
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

fn parse_update_mutation(pair: pest::iterators::Pair<Rule>) -> Result<(MutationOp, Vec<Field>)> {
    let mut collection = String::new();
    let mut filter = None;
    let mut data = Value::Null;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::update_args => {
                for arg in parse_arguments_list(inner)? {
                    match arg.name.as_str() {
                        "collection" => if let Value::String(s) = arg.value { collection = s; }
                        "where" => filter = Some(value_to_filter(arg.value)?),
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

    Ok((MutationOp::Update { collection, filter, data }, selection_set))
}

fn parse_delete_mutation(pair: pest::iterators::Pair<Rule>) -> Result<(MutationOp, Vec<Field>)> {
    let mut collection = String::new();
    let mut filter = None;
    let mut selection_set = Vec::new();

    for inner in pair.into_inner() {
        match inner.as_rule() {
            Rule::delete_args => {
                for arg in parse_arguments_list(inner)? {
                    match arg.name.as_str() {
                        "collection" => if let Value::String(s) = arg.value { collection = s; }
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

fn parse_enqueue_job_mutation(pair: pest::iterators::Pair<Rule>) -> Result<(MutationOp, Vec<Field>)> {
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
                        "jobType" => if let Value::String(s) = arg.value { job_type = s; }
                        "payload" => payload = arg.value,
                        "priority" => if let Value::Enum(s) = arg.value {
                            priority = match s.as_str() {
                                "LOW" => JobPriority::Low,
                                "HIGH" => JobPriority::High,
                                "CRITICAL" => JobPriority::Critical,
                                _ => JobPriority::Normal,
                            };
                        }
                        "scheduledAt" => if let Value::String(s) = arg.value { scheduled_at = Some(s); }
                        "maxRetries" => if let Value::Int(n) = arg.value { max_retries = Some(n as u32); }
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

    Ok((MutationOp::EnqueueJob { job_type, payload, priority, scheduled_at, max_retries }, selection_set))
}

fn parse_transaction_block(pair: pest::iterators::Pair<Rule>) -> Result<(MutationOp, Vec<Field>)> {
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
            let unquoted = if s.starts_with("\"\"\"") { &s[3..s.len()-3] } else { &s[1..s.len()-1] };
            Ok(Value::String(unquoted.to_string()))
        }
        Rule::number => {
            let s = pair.as_str();
            if s.contains('.') || s.contains('e') || s.contains('E') {
                Ok(Value::Float(s.parse().unwrap_or(0.0)))
            } else {
                Ok(Value::Int(s.parse().unwrap_or(0)))
            }
        }
        Rule::boolean => Ok(Value::Boolean(pair.as_str() == "true")),
        Rule::null => Ok(Value::Null),
        Rule::variable => Ok(Value::Variable(pair.as_str().trim_start_matches('$').to_string())),
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
                key = s[1..s.len()-1].to_string();
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
                    "and" => if let Value::Array(arr) = val {
                        let sub: Result<Vec<_>> = arr.into_iter().map(value_to_filter).collect();
                        filters.push(Filter::And(sub?));
                    }
                    "or" => if let Value::Array(arr) = val {
                        let sub: Result<Vec<_>> = arr.into_iter().map(value_to_filter).collect();
                        filters.push(Filter::Or(sub?));
                    }
                    "not" => filters.push(Filter::Not(Box::new(value_to_filter(val)?))),
                    field => if let Value::Object(ops) = val {
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
                                _ => continue,
                            };
                            filters.push(f);
                        }
                    }
                }
            }
            if filters.len() == 1 { Ok(filters.remove(0)) } else { Ok(Filter::And(filters)) }
        }
        _ => Err(AuroraError::Protocol("Filter must be an object".to_string())),
    }
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
        let query = r#"query GetActiveUsers { users(where: { active: { eq: true } }) { id name } }"#;
        let result = parse(query);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_mutation() {
        let query = r#"mutation { insertInto(collection: "users", data: { name: "John" }) { id } }"#;
        let result = parse(query);
        assert!(result.is_ok());
    }
}
