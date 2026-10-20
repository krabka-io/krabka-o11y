use std::collections::BTreeMap;

use arrow::array::{Array as _, FixedSizeBinaryArray};

pub(crate) fn group_rows_by_trace_id(
    trace_ids: &FixedSizeBinaryArray,
) -> BTreeMap<[u8; 16], Vec<usize>> {
    let mut by_trace: BTreeMap<[u8; 16], Vec<usize>> = BTreeMap::new();
    for row in 0..trace_ids.len() {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        by_trace.entry(trace_id).or_default().push(row);
    }
    by_trace
}
