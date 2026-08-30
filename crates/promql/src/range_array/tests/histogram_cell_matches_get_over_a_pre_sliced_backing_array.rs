use super::*;

#[test]
pub(crate) fn histogram_cell_matches_get_over_a_pre_sliced_backing_array() {
    let rows = native_histogram_rows();
    let batch = encode_native_histograms(&rows).unwrap();
    let histograms = Arc::new(StructArray::from(
        batch
            .schema()
            .fields()
            .iter()
            .cloned()
            .zip(batch.columns().iter().cloned())
            .collect::<Vec<_>>(),
    )) as ArrayRef;
    let sliced = histograms.slice(1, 2);
    let range_array = RangeArray::from_ranges(sliced, [(0_u32, 1_u32), (1, 1)]).unwrap();

    let via_get = range_array.get(0).unwrap();
    let via_get = via_get.as_any().downcast_ref::<StructArray>().unwrap();
    let via_get_batch = RecordBatch::from(via_get.clone());
    let decoded = decode_native_histograms(&via_get_batch).unwrap();
    let cell = range_array.histogram_cell(0).unwrap();

    assert2::assert!(decoded == vec![rows[1].clone()]);
    check!(cell.schema_slice() == [rows[1].2.schema]);
    check!(cell.count_slice() == [rows[1].2.count]);
    check!(cell.positive_counts(0) == Some(rows[1].2.positive_counts.as_slice()));
    check!(cell.negative_counts(0) == Some(rows[1].2.negative_counts.as_slice()));
}
