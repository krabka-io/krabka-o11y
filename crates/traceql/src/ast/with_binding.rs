use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct WithBinding {
    pub name: String,
    pub expr: FieldExpr,
}
