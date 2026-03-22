use aurora_db::parser::validator::ErrorCode;
use aurora_db::parser::{parse_with_variables, validator};
use aurora_db::types::{Collection, FieldDefinition, FieldType, ScalarType};
use serde_json::json;
use std::collections::HashMap;

struct MockSchemaProvider {
    collections: HashMap<String, Collection>,
}

impl MockSchemaProvider {
    fn new() -> Self {
        Self {
            collections: HashMap::new(),
        }
    }

    fn add(&mut self, col: Collection) {
        self.collections.insert(col.name.clone(), col);
    }
}

impl validator::SchemaProvider for MockSchemaProvider {
    fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.get(name)
    }
}

fn create_test_schema() -> MockSchemaProvider {
    let mut schema = MockSchemaProvider::new();

    let mut fields = HashMap::new();
    fields.insert(
        "name".into(),
        FieldDefinition {
            field_type: FieldType::Scalar(ScalarType::String),
            unique: false,
            indexed: false,
            nullable: false,
        ..Default::default()
        },
    );
    fields.insert(
        "age".into(),
        FieldDefinition {
            field_type: FieldType::Scalar(ScalarType::Int),
            unique: false,
            indexed: false,
            nullable: true,
        ..Default::default()
        },
    );

    schema.add(Collection {
        name: "users".into(),
        fields,
    });

    schema
}

#[test]
fn test_validation_type_mismatch() {
    let schema = create_test_schema();

    // Attempt to insert string into int field 'age'
    let mutation = r#"
        mutation {
            insertInto(collection: "users", data: { name: "Alice", age: "twenty" }) { id }
        }
    "#;

    let doc = parse_with_variables(mutation, json!({})).unwrap();
    let res = validator::validate_document(&doc, &schema, HashMap::new());

    assert!(res.is_err());
    let errors = res.unwrap_err();
    assert_eq!(errors[0].code, ErrorCode::TypeMismatch);
    assert!(errors[0].message.contains("Type mismatch for field 'age'"));
}

#[test]
fn test_validation_unknown_field() {
    let schema = create_test_schema();

    // Attempt to insert unknown field 'email'
    let mutation = r#"
        mutation {
            insertInto(collection: "users", data: { name: "Alice", email: "alice@example.com" }) { id }
        }
    "#;

    let doc = parse_with_variables(mutation, json!({})).unwrap();
    let res = validator::validate_document(&doc, &schema, HashMap::new());

    assert!(res.is_err());
    let errors = res.unwrap_err();
    assert_eq!(errors[0].code, ErrorCode::UnknownField);
    assert!(
        errors[0]
            .message
            .contains("not defined in collection 'users'")
    );
}

#[test]
fn test_validation_not_an_object() {
    let schema = create_test_schema();

    // Attempt to pass a string as data
    let mutation = r#"
        mutation {
            insertInto(collection: "users", data: "invalid_data") { id }
        }
    "#;

    let doc = parse_with_variables(mutation, json!({})).unwrap();
    let res = validator::validate_document(&doc, &schema, HashMap::new());

    assert!(res.is_err());
    let errors = res.unwrap_err();
    assert_eq!(errors[0].code, ErrorCode::TypeMismatch);
}

#[test]
fn test_validation_missing_required() {
    let schema = create_test_schema();

    // Attempt to insert without required 'name' field (nullable=false)
    let mutation = r#"
        mutation {
            insertInto(collection: "users", data: { age: 25 }) { id }
        }
    "#;

    let doc = parse_with_variables(mutation, json!({})).unwrap();
    let res = validator::validate_document(&doc, &schema, HashMap::new());

    assert!(res.is_err());
    let errors = res.unwrap_err();
    assert_eq!(errors[0].code, ErrorCode::InvalidInput);
    assert!(errors[0].message.contains("Missing required field 'name'"));
}

#[test]
fn test_validation_array_type() {
    let mut schema = create_test_schema();

    // Add array field to schema manually for test
    if let Some(col) = schema.collections.get_mut("users") {
        col.fields.insert(
            "tags".into(),
            FieldDefinition {
                field_type: FieldType::Array(ScalarType::String),
                unique: false,
                indexed: false,
                nullable: true,
            ..Default::default()
            },
        );
    }

    // Invalid array content (int inside string array)
    let mutation = r#"
        mutation {
            insertInto(collection: "users", data: { name: "Bob", tags: ["valid", 123] }) { id }
        }
    "#;

    let doc = parse_with_variables(mutation, json!({})).unwrap();
    let res = validator::validate_document(&doc, &schema, HashMap::new());

    assert!(res.is_err());
    let errors = res.unwrap_err();
    assert_eq!(errors[0].code, ErrorCode::TypeMismatch);
    assert!(errors[0].message.contains("Type mismatch for field 'tags'"));
}
