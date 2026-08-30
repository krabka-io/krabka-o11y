use super::*;

#[test]
pub(crate) fn paired_builder_rejects_length_mismatch() {
    let values = Float64Array::from(vec![1.0, 2.0, 3.0]);
    let timestamps = Int64Array::from(vec![0_i64, 1]);
    assert2::assert!(RangeArray::from_paired_ranges(values, timestamps, [(0_u32, 2_u32)]).is_err());
}
