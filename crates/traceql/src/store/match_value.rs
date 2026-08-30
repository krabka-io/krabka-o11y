use super::*;

#[derive(Clone, Debug, PartialEq)]
pub enum MatchValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
}
