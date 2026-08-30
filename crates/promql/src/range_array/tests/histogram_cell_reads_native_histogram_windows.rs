use super::*;

#[test]
pub(crate) fn histogram_cell_reads_native_histogram_windows() {
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
    let range_array = RangeArray::from_ranges(histograms, [(0_u32, 2_u32), (2, 1)]).unwrap();

    let first_cell = range_array.histogram_cell(0).unwrap();
    check!(first_cell.len() == 2);
    check!(!first_cell.is_empty());
    check!(first_cell.schema_slice() == [2, -53]);
    check!(first_cell.reset_hint_slice() == [ResetHint::No.as_i8(), ResetHint::Gauge.as_i8()]);
    check!(first_cell.zero_threshold_slice() == [1e-128, 0.25]);
    check!(first_cell.zero_count_slice() == [3.0, 0.5]);
    check!(first_cell.count_slice() == [10.0, 4.0]);
    check!(first_cell.sum_slice() == [42.5, 7.5]);
    check!(first_cell.is_float(0) == Some(false));
    check!(first_cell.is_float(1) == Some(true));
    check!(first_cell.is_float(2).is_none());

    let first_positive_spans = first_cell.positive_spans(0).unwrap();
    check!(first_positive_spans.offsets() == [0]);
    check!(first_positive_spans.lengths() == [2]);
    check!(first_cell.positive_counts(0) == Some(&[4.0, 6.0][..]));
    let second_negative_spans = first_cell.negative_spans(1).unwrap();
    check!(second_negative_spans.offsets() == [-1]);
    check!(second_negative_spans.lengths() == [1]);
    check!(first_cell.negative_counts(1) == Some(&[0.75][..]));
    check!(first_cell.custom_values(0).is_none());
    check!(first_cell.custom_values(1) == Some(&[0.5, 1.0, 2.0][..]));
    check!(first_cell.start_timestamp_ms(0).is_none());
    check!(first_cell.start_timestamp_ms(1) == Some(123));

    let second_cell = range_array.histogram_cell(1).unwrap();
    check!(second_cell.len() == 1);
    check!(second_cell.positive_spans(0).unwrap().is_empty());
    check!(second_cell.positive_counts(0) == Some(&[][..]));
    check!(range_array.histogram_cell(2).is_none());
    check!(range_array.value_slice(0).is_none());

    let decoded = decode_native_histograms(&batch).unwrap();
    assert2::assert!(decoded == rows);
}
