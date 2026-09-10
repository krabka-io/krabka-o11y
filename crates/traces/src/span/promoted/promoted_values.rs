use super::{Array, ListArray, RecordBatch, TracesError};

/// Read one scalar per row out of a `List<List<T>>` attribute value column.
///
/// `slots` names the attribute position to read in each row, as returned by
/// [`super::promoted_attr_slots`]. A row whose attribute is absent, or whose
/// value list is empty or holds a different type, reads as `None`, which is
/// exactly what the write path stores in the dedicated column.
pub(crate) fn promoted_values<A: Array + 'static, T>(
    batch: &RecordBatch,
    column: &str,
    slots: &[Option<usize>],
    read: impl Fn(&A, usize) -> T,
) -> Result<Vec<Option<T>>, TracesError> {
    let values = batch
        .column_by_name(column)
        .and_then(|values| values.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| TracesError::Block(format!("{column} is not a list")))?;

    let mut out = Vec::with_capacity(slots.len());
    for (row, slot) in slots.iter().enumerate() {
        out.push(match slot {
            Some(slot) => row_value(values, column, row, *slot, &read)?,
            None => None,
        });
    }
    Ok(out)
}

fn row_value<A: Array + 'static, T>(
    values: &ListArray,
    column: &str,
    row: usize,
    slot: usize,
    read: &impl Fn(&A, usize) -> T,
) -> Result<Option<T>, TracesError> {
    if values.is_null(row) {
        return Ok(None);
    }
    let row_values = values.value(row);
    let row_values = row_values
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| TracesError::Block(format!("{column} row is not a list")))?;
    if slot >= row_values.len() || row_values.is_null(slot) {
        return Ok(None);
    }
    let attr_values = row_values.value(slot);
    // A value list of another type belongs to another attribute of the same
    // row: only one of the four typed columns is populated per attribute.
    let Some(attr_values) = attr_values.as_any().downcast_ref::<A>() else {
        return Ok(None);
    };
    Ok((0..attr_values.len())
        .find(|idx| !attr_values.is_null(*idx))
        .map(|idx| read(attr_values, idx)))
}
