/// One typed tag value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TypedValue {
    pub type_: String,
    pub value: String,
}

impl TypedValue {
    pub fn new(type_: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            type_: type_.into(),
            value: value.into(),
        }
    }
}
