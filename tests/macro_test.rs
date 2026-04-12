use aurora_db::parser::ast::Value;
use aurora_db::{doc, value};

#[test]
fn test_value_macro() {
    let val = value!({
        "id": 123,
        "name": "Jane",
        "active": true,
        "tags": ["admin", "user"]
    });

    if let Value::Object(map) = val {
        assert_eq!(map.get("id").unwrap(), &Value::Int(123));
        assert_eq!(map.get("name").unwrap(), &Value::String("Jane".to_string()));
        assert_eq!(map.get("active").unwrap(), &Value::Boolean(true));

        if let Value::Array(arr) = map.get("tags").unwrap() {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], Value::String("admin".to_string()));
            assert_eq!(arr[1], Value::String("user".to_string()));
        } else {
            panic!("Expected array for tags");
        }
    } else {
        panic!("Expected object");
    }
}

#[test]
fn test_doc_macro() {
    let (query, options) = doc!("query users { filter id == $id }", {
        "id": 456
    });

    assert_eq!(query, "query users { filter id == $id }");
    assert_eq!(options.variables.get("id").unwrap(), &Value::Int(456));
}
