use super::{BinModifier, Result, VectorMatchCardinality, PromqlError};

/// Rejects a modifier that only a set operator may carry.
///
/// Unreachable by construction, and so a permanent mutation survivor:
/// `promql-parser` sets `ManyToMany` only for `and`/`or`/`unless`, and
/// `combine_instant_binary` routes those away before calling this. It stays as
/// the guard for that invariant, not as live validation.
pub(crate) fn validate_binary_modifier(modifier: Option<&BinModifier>) -> Result<()> {
    let Some(modifier) = modifier else {
        return Ok(());
    };
    if matches!(modifier.card, VectorMatchCardinality::ManyToMany) {
        return Err(PromqlError::Unsupported(
            "many-to-many vector matching is only valid for set operators".to_string(),
        ));
    }
    Ok(())
}
