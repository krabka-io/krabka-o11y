#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Float(f64),
    Duration(i64),
    Bool(bool),
    Nil,
}
