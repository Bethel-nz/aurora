#[macro_export]
macro_rules! value {
    // Hide unexpected uses of the macro
    (null) => {
        $crate::parser::ast::Value::Null
    };
    (true) => {
        $crate::parser::ast::Value::Boolean(true)
    };
    (false) => {
        $crate::parser::ast::Value::Boolean(false)
    };
    ([]) => {
        $crate::parser::ast::Value::Array(std::vec::Vec::new())
    };
    ([ $( $tt:tt )+ ]) => {
        $crate::array!( $( $tt )+ )
    };
    ({}) => {
        $crate::parser::ast::Value::Object(std::collections::HashMap::new())
    };
    ({ $( $k:tt : $v:tt ),* $(,)? }) => {
        $crate::object!( $( $k : $v ),* )
    };
    // Handle already constructed Values (nested macro calls)
    ($val:expr) => {
        $crate::parser::ast::Value::from($val)
    };
}

#[macro_export]
macro_rules! object {
    ({ $( $k:tt : $v:tt ),* $(,)? }) => {
        $crate::object!( $( $k : $v ),* )
    };
    ($($k:expr => $v:tt),* $(,)?) => {
        $crate::parser::ast::Value::Object(std::collections::HashMap::from([
            $( ($k.into(), $crate::value!($v)) ),*
        ]))
    };
    ($($k:tt : $v:tt),* $(,)?) => {
        $crate::parser::ast::Value::Object(std::collections::HashMap::from([
            $( ($k.into(), $crate::value!($v)) ),*
        ]))
    };
}

#[macro_export]
macro_rules! array {
    ($($val:expr),* $(,)?) => {
        $crate::parser::ast::Value::Array(std::vec![
            $( $crate::value!($val) ),*
        ])
    };
}

/// Creates a `(String, ExecutionOptions)` tuple to be executed by `db.execute()`.
/// Example: `db.execute(doc!("query users { filter id == $id }", { "id": 123 }))`
#[macro_export]
macro_rules! doc {
    ($query:expr) => {
        (
            $query.to_string(),
            $crate::parser::executor::ExecutionOptions::default(),
        )
    };
    ($query:expr, { $( $k:tt : $v:tt ),* $(,)? }) => {
        (
            $query.to_string(),
            $crate::parser::executor::ExecutionOptions {
                variables: std::collections::HashMap::from([
                    $( ($k.into(), $crate::value!($v)) ),*
                ]),
                ..std::default::Default::default()
            }
        )
    };
}
