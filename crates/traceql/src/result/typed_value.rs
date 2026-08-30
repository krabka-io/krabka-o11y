/// One typed tag value.
#[derive(Clone, Debug, PartialEq)]
pub struct TypedValue {
    pub type_: String,
    pub value: String,
}
