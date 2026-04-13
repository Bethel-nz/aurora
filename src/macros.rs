/// Constructs an Aurora `Value` from Rust literals.
///
/// This macro provides a JSON-like syntax for creating strongly-typed Aurora values
/// without the overhead of JSON serialization. It is used internally by `object!`
/// and `array!`, but can also be used directly.
///
/// # Examples
///
/// ```
/// # use aurora_db::value;
/// let v_null = value!(null);
/// let v_bool = value!(true);
/// let v_num = value!(42);
/// let v_str = value!("hello");
/// ```
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

/// Constructs an Aurora `Value::Object` (HashMap) from key-value pairs.
///
/// Supports both JSON-style `{ "key": value }` and arrow-style `{ "key" => value }` syntax.
///
/// # Examples
///
/// ```
/// # use aurora_db::object;
/// let user = object!({
///     "id": "u1",
///     "name": "Alice",
///     "active": true
/// });
/// ```
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

/// Constructs an Aurora `Value::Array` (Vec) from a list of values.
///
/// # Examples
///
/// ```
/// # use aurora_db::array;
/// let tags = array!["rust", "database", 2024];
/// ```
#[macro_export]
macro_rules! array {
    ($($val:expr),* $(,)?) => {
        $crate::parser::ast::Value::Array(std::vec![
            $( $crate::value!($val) ),*
        ])
    };
}

/// Creates a `(String, ExecutionOptions)` tuple to be executed by `db.execute()`.
///
/// This macro is the primary way to execute parametrized AQL queries and mutations
/// in Rust. It automatically binds Rust variables to AQL variables using the native
/// Aurora `Value` system, ensuring safety and performance.
///
/// # Examples
///
/// ```
/// # use aurora_db::{Aurora, doc};
/// # async fn example(db: Aurora) -> Result<(), Box<dyn std::error::Error>> {
/// let min_age = 18;
///
/// // Simple query without variables
/// let res = db.execute(doc!("query { users { id name } }")).await?;
///
/// // Query with variables
/// let res = db.execute(doc!(
///     "query($minAge: Int) {
///         users(where: { age: { gte: $minAge } }) {
///             name
///         }
///     }",
///     { "minAge": min_age }
/// )).await?;
/// # Ok(())
/// # }
/// ```
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
