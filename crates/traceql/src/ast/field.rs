use super::Scope;

#[derive(Clone, Debug, PartialEq, Eq)]
/// A scoped trace attribute reference.
pub struct Field {
    /// The resource, span, event, link, instrumentation, or intrinsic scope.
    pub scope: Scope,
    /// The attribute key inside the scope.
    pub key: String,
}
