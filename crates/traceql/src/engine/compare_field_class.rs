use super::{Field, Intrinsic, Result, Scope, TraceqlError};

/// A span attribute, a resource attribute, or a selection-evaluable intrinsic.
///
/// The compare code classifies a field into this enum for per-row lookup. The
/// classification rejects parent references, which pinned Tempo does not support.
pub(crate) enum CompareFieldClass {
    /// A span-scoped or resource-scoped attribute, keyed by its raw key.
    /// `Both` matches either scope. `Span` and `Resource` pin the scope.
    Attr { scope: Scope, key: String },
    /// A selection-evaluable intrinsic with its row value.
    Intrinsic(Intrinsic),
}

pub(crate) fn compare_field_class(field: &Field) -> Result<CompareFieldClass> {
    match &field.scope {
        Scope::Both
        | Scope::Span
        | Scope::Resource
        | Scope::Event
        | Scope::Link
        | Scope::Instrumentation => Ok(CompareFieldClass::Attr {
            scope: field.scope.clone(),
            key: field.key.clone(),
        }),
        Scope::Intrinsic(intrinsic) => Ok(CompareFieldClass::Intrinsic(intrinsic.clone())),
        other @ Scope::Parent => Err(TraceqlError::Unsupported(format!(
            "compare() selection does not support {other:?}"
        ))),
    }
}
