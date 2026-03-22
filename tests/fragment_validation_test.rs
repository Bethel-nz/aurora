use aurora_db::parser::{
    parse,
    validator::{ErrorCode, InMemorySchema, validate_document},
};
use aurora_db::types::{Collection, FieldDefinition, FieldType, ScalarType};
use std::collections::HashMap;

fn create_test_schema() -> InMemorySchema {
    let mut schema = InMemorySchema::new();

    // Users collection
    schema.add_collection(Collection {
        name: "users".to_string(),
        fields: vec![
            (
                "id".to_string(),
                FieldDefinition {
                    field_type: FieldType::Scalar(ScalarType::Int),
                    nullable: false,
                    unique: true,
                    indexed: true,
                ..Default::default()
                },
            ),
            (
                "name".to_string(),
                FieldDefinition {
                    field_type: FieldType::Scalar(ScalarType::String),
                    nullable: false,
                    unique: false,
                    indexed: false,
                ..Default::default()
                },
            ),
            (
                "email".to_string(),
                FieldDefinition {
                    field_type: FieldType::Scalar(ScalarType::String),
                    nullable: false,
                    unique: true,
                    indexed: true,
                ..Default::default()
                },
            ),
            (
                "age".to_string(),
                FieldDefinition {
                    field_type: FieldType::Scalar(ScalarType::Int),
                    nullable: true,
                    unique: false,
                    indexed: false,
                ..Default::default()
                },
            ),
        ]
        .into_iter()
        .collect(),
    });

    // Posts collection
    schema.add_collection(Collection {
        name: "posts".to_string(),
        fields: vec![
            (
                "id".to_string(),
                FieldDefinition {
                    field_type: FieldType::Scalar(ScalarType::Int),
                    nullable: false,
                    unique: true,
                    indexed: true,
                ..Default::default()
                },
            ),
            (
                "title".to_string(),
                FieldDefinition {
                    field_type: FieldType::Scalar(ScalarType::String),
                    nullable: false,
                    unique: false,
                    indexed: false,
                ..Default::default()
                },
            ),
            (
                "content".to_string(),
                FieldDefinition {
                    field_type: FieldType::Scalar(ScalarType::String),
                    nullable: false,
                    unique: false,
                    indexed: false,
                ..Default::default()
                },
            ),
        ]
        .into_iter()
        .collect(),
    });

    schema
}

#[test]
fn test_valid_fragment_spread() {
    let schema = create_test_schema();
    let query = r#"
        fragment UserFields on users {
            name
            email
        }

        query {
            users {
                id
                ...UserFields
            }
        }
    "#;

    let doc = parse(query).unwrap();
    let result = validate_document(&doc, &schema, HashMap::new());

    assert!(
        result.is_ok(),
        "Valid fragment spread should pass validation"
    );
}

#[test]
fn test_fragment_type_mismatch() {
    let schema = create_test_schema();
    let query = r#"
        fragment PostFields on posts {
            title
        }

        query {
            users {
                name
                ...PostFields
            }
        }
    "#;

    let doc = parse(query).unwrap();
    let result = validate_document(&doc, &schema, HashMap::new());

    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].code, ErrorCode::TypeMismatch);
    assert!(
        errors[0]
            .message
            .contains("Fragment 'PostFields' is defined on 'posts' but used on 'users'")
    );
}

#[test]
fn test_undefined_fragment() {
    let schema = create_test_schema();
    let query = r#"
        query {
            users {
                name
                ...MissingFragment
            }
        }
    "#;

    let doc = parse(query).unwrap();
    let result = validate_document(&doc, &schema, HashMap::new());

    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].code, ErrorCode::UnknownFragment);
    assert!(
        errors[0]
            .message
            .contains("Fragment 'MissingFragment' is not defined")
    );
}

#[test]
fn test_fragment_with_invalid_fields() {
    let schema = create_test_schema();
    // 'salary' does not exist on users
    let query = r#"
        fragment UserFields on users {
            name
            salary
        }

        query {
            users {
                ...UserFields
            }
        }
    "#;

    let doc = parse(query).unwrap();
    let result = validate_document(&doc, &schema, HashMap::new());

    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(
        errors
            .iter()
            .any(|e| e.code == ErrorCode::UnknownField && e.message.contains("salary"))
    );
}

#[test]
fn test_nested_fragment_spreads() {
    let schema = create_test_schema();
    let query = r#"
        fragment BasicInfo on users {
            name
        }

        fragment DetailedInfo on users {
            ...BasicInfo
            email
            age
        }

        query {
            users {
                ...DetailedInfo
            }
        }
    "#;

    let doc = parse(query).unwrap();
    let result = validate_document(&doc, &schema, HashMap::new());

    assert!(
        result.is_ok(),
        "Nested fragment spreads should allow valid fields"
    );
}
