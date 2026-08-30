/// A generic attribute value list. Scalars are represented as one-element lists.
#[derive(Clone, Debug, PartialEq)]
pub enum AttrValue {
    Str(Vec<String>),
    Int(Vec<i64>),
    Double(Vec<f64>),
    Bool(Vec<bool>),
}
