use super::{Array, BooleanArray, ListArray, StringArray, TracesError};

/// The position of `key` in one row's generic attribute list, or `None` when
/// the row does not carry it.
///
/// Array-valued attributes are skipped, because the write path promotes only
/// scalars: [`krabka_blockstore::encode_span_rows_with_promoted_attrs`] leaves
/// the dedicated column null for a row whose matching attribute holds a list.
pub(crate) fn row_attr_slot(
    keys: &ListArray,
    is_array: Option<&ListArray>,
    row: usize,
    key: &str,
) -> Result<Option<usize>, TracesError> {
    if keys.is_null(row) {
        return Ok(None);
    }
    let row_keys = keys.value(row);
    let row_keys = row_keys
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TracesError::Block("attribute keys row is not Utf8".into()))?;

    let row_flags = match is_array {
        Some(flags) if !flags.is_null(row) => Some(flags.value(row)),
        _ => None,
    };
    let row_flags =
        match row_flags.as_ref() {
            Some(flags) => Some(flags.as_any().downcast_ref::<BooleanArray>().ok_or_else(
                || TracesError::Block("attribute is-array row is not Boolean".into()),
            )?),
            None => None,
        };

    for idx in 0..row_keys.len() {
        if row_keys.is_null(idx) || row_keys.value(idx) != key {
            continue;
        }
        let holds_array = row_flags
            .is_some_and(|flags| idx < flags.len() && !flags.is_null(idx) && flags.value(idx));
        if holds_array {
            continue;
        }
        return Ok(Some(idx));
    }
    Ok(None)
}
