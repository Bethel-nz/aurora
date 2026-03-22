use aurora_db::parser::ast::{Document, Mutation, MutationOp, MutationOperation, Operation, Value};
use aurora_db::parser::validator::{InMemorySchema, validate_document};
use aurora_db::types::{Collection, FieldDefinition, FieldType};
use std::collections::HashMap;

#[test]
fn test_nested_object_validation() {
    // 1. Define schema with Nested type
    let mut address_fields = HashMap::new();
    address_fields.insert(
        "street".to_string(),
        FieldDefinition {
            field_type: FieldType::SCALAR_STRING,
            unique: false,
            indexed: false,
            nullable: false,
        ..Default::default()
        },
    );
    address_fields.insert(
        "zip".to_string(),
        FieldDefinition {
            field_type: FieldType::SCALAR_INT,
            unique: false,
            indexed: false,
            nullable: true,
        ..Default::default()
        },
    );

    let mut user_fields = HashMap::new();
    user_fields.insert(
        "name".to_string(),
        FieldDefinition {
            field_type: FieldType::SCALAR_STRING,
            unique: false,
            indexed: false,
            nullable: false,
        ..Default::default()
        },
    );
    user_fields.insert(
        "address".to_string(),
        FieldDefinition {
            field_type: FieldType::Nested(Box::new(address_fields)),
            unique: false,
            indexed: false,
            nullable: false,
        ..Default::default()
        },
    );

    let collection = Collection {
        name: "users".to_string(),
        fields: user_fields,
    };

    let mut schema = InMemorySchema::new();
    schema.add_collection(collection);

    // 2. Validate Valid Document
    let mut valid_address = HashMap::new();
    valid_address.insert(
        "street".to_string(),
        Value::String("123 Main St".to_string()),
    );
    valid_address.insert("zip".to_string(), Value::Int(90210));

    let mut valid_data = HashMap::new();
    valid_data.insert("name".to_string(), Value::String("Alice".to_string()));
    valid_data.insert("address".to_string(), Value::Object(valid_address));

    let valid_doc = Document {
        operations: vec![Operation::Mutation(Mutation {
            name: None,
            variable_definitions: vec![],
            directives: vec![],
            variables_values: HashMap::new(),
            operations: vec![MutationOperation {
                alias: None,
                directives: vec![],
                selection_set: vec![],
                operation: MutationOp::Insert {
                    collection: "users".to_string(),
                    data: Value::Object(valid_data),
                },
            }],
        })],
    };

    let result = validate_document(&valid_doc, &schema, HashMap::new());
    println!("1. Valid Document Result: {:?}", result);
    assert!(
        result.is_ok(),
        "Valid nested object should pass validation: {:?}",
        result.err()
    );

    // 3. Validate Invalid Document (Missing required nested field 'street')
    let mut invalid_address = HashMap::new();
    invalid_address.insert("zip".to_string(), Value::Int(90210));

    let mut invalid_data = HashMap::new();
    invalid_data.insert("name".to_string(), Value::String("Bob".to_string()));
    invalid_data.insert("address".to_string(), Value::Object(invalid_address));

    let invalid_doc = Document {
        operations: vec![Operation::Mutation(Mutation {
            name: None,
            variable_definitions: vec![],
            directives: vec![],
            variables_values: HashMap::new(),
            operations: vec![MutationOperation {
                alias: None,
                directives: vec![],
                selection_set: vec![],
                operation: MutationOp::Insert {
                    collection: "users".to_string(),
                    data: Value::Object(invalid_data),
                },
            }],
        })],
    };

    let result = validate_document(&invalid_doc, &schema, HashMap::new());
    println!("2. Invalid Document (Missing Field) Result: {:?}", result);
    assert!(
        result.is_err(),
        "Invalid nested object (missing field) should fail validation"
    );

    // 4. Validate Invalid Document (Type mismatch in nested field)
    let mut type_mismatch_address = HashMap::new();
    type_mismatch_address.insert("street".to_string(), Value::Int(123)); // Should be string

    let mut mismatch_data = HashMap::new();
    mismatch_data.insert("name".to_string(), Value::String("Charlie".to_string()));
    mismatch_data.insert("address".to_string(), Value::Object(type_mismatch_address));

    let mismatch_doc = Document {
        operations: vec![Operation::Mutation(Mutation {
            name: None,
            variable_definitions: vec![],
            directives: vec![],
            variables_values: HashMap::new(),
            operations: vec![MutationOperation {
                alias: None,
                directives: vec![],
                selection_set: vec![],
                operation: MutationOp::Insert {
                    collection: "users".to_string(),
                    data: Value::Object(mismatch_data),
                },
            }],
        })],
    };

    let result = validate_document(&mismatch_doc, &schema, HashMap::new());
    println!("3. Invalid Document (Type Mismatch) Result: {:?}", result);
    assert!(
        result.is_err(),
        "Invalid nested object (type mismatch) should fail validation"
    );
}
