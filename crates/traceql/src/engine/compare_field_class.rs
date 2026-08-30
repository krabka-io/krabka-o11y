use super::{Field, Intrinsic, Result, Scope, TraceqlError};

/// A span attribute, a resource attribute, or a selection-evaluable intrinsic.
///
/// The compare code classifies a field into this enum for per-row lookup. The
/// classification returns an `Unsupported` error for scopes that the
/// single-span compare evaluator cannot resolve. Those scopes are parent,
/// event, and link, plus non-selection intrinsics such as trace-level fields
/// and nested-set fields.
pub(crate) enum CompareFieldClass {
    /// A span-scoped or resource-scoped attribute, keyed by its raw key.
    /// `Both` matches either scope. `Span` and `Resource` pin the scope.
    Attr { scope: Scope, key: String },
    /// A selection-evaluable intrinsic with its row value.
    Intrinsic(Intrinsic),
}

pub(crate) fn compare_field_class(field: &Field) -> Result<CompareFieldClass> {
    match &field.scope {
        Scope::Both | Scope::Span | Scope::Resource => Ok(CompareFieldClass::Attr {
            scope: field.scope.clone(),
            key: field.key.clone(),
        }),
        Scope::Intrinsic(
            intrinsic @ (Intrinsic::Name
            | Intrinsic::Status
            | Intrinsic::StatusMessage
            | Intrinsic::Kind
            | Intrinsic::Duration),
        ) => Ok(CompareFieldClass::Intrinsic(intrinsic.clone())),
        other => Err(TraceqlError::Unsupported(format!(
            "compare() selection does not support {other:?}"
        ))),
    }
}
