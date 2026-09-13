use super::FieldExpr;

#[derive(Clone, Debug, PartialEq)]
/// A named field expression introduced by a `TraceQL` `with` pipeline.
pub struct WithBinding {
    /// The binding name.
    pub name: String,
    /// The expression assigned to the name.
    pub expr: FieldExpr,
}
