#[derive(Clone, Debug, PartialEq)]
/// A scalar literal in a `TraceQL` field expression.
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Duration(i64),
    Bool(bool),
    Nil,
    /// Runtime array attribute; array literals are not part of the parser syntax.
    Array(Vec<Value>),
}
