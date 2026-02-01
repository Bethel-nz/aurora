use aurora_db::parser::parse;

#[test]
fn test_parse_error_location() {
    let invalid_query = r#"
        query {
            users {
                name
                age
            # Missing closing brace here
    "#;

    let res = parse(invalid_query);
    assert!(res.is_err(), "Expected parse error");

    let err = res.unwrap_err();
    println!("Error: {}", err);

    // Verify location info is present
    assert!(err.line.is_some(), "Expected line number");
    assert!(err.column.is_some(), "Expected column number");

    // Line 7 is where the closing brace should be (or EOF is reached)
    // Detailed pest error usually points to the end of input or specific token
    let line = err.line.unwrap();
    assert!(line >= 5, "Error should be reported near the end (line 5+)");
}

#[test]
fn test_parse_error_explicit_token() {
    // Invalid token "undefined_keyword" at start
    let invalid_query = "undefined_keyword { }";
    let res = parse(invalid_query);

    assert!(res.is_err());
    let err = res.unwrap_err();

    assert_eq!(err.line, Some(1));
    // Column might be 1 or slightly after depending on grammar
    assert!(err.column.is_some());
}
