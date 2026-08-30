use super::{Deserialize, Serialize};

/// A typed attribute value. Block encoding preserves arrays.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AttrValue {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}
