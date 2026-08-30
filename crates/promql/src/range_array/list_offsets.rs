use super::{Array, ListArray};

pub(crate) fn list_offsets(list: &ListArray, row: usize) -> Option<(usize, usize)> {
    if list.is_null(row) {
        return None;
    }
    let offsets = list.value_offsets();
    let start = usize::try_from(*offsets.get(row)?).ok()?;
    let end = usize::try_from(*offsets.get(row + 1)?).ok()?;
    Some((start, end))
}
