use super::{ListArray, HistogramSpanView, list_offsets, Array, StructArray, Int32Array, UInt32Array};

pub(crate) fn span_list_value(list: &ListArray, row: usize) -> Option<HistogramSpanView<'_>> {
    let (start, end) = list_offsets(list, row)?;
    let values = list.values().as_any().downcast_ref::<StructArray>()?;
    let offsets = values
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()?
        .values();
    let lengths = values
        .column(1)
        .as_any()
        .downcast_ref::<UInt32Array>()?
        .values();
    Some(HistogramSpanView {
        offsets: &offsets[start..end],
        lengths: &lengths[start..end],
    })
}
