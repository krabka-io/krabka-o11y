use super::*;

#[test]
pub(crate) fn typed_accessor_matches_get_over_a_pre_sliced_backing_array() {
    // Build a RangeArray over an already-sliced backing array; the typed
    // zero-copy accessor must agree with the `get()` re-slice path.
    let full = Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0])) as ArrayRef;
    let sliced = full.slice(2, 3); // logical [2.0, 3.0, 4.0]
    let range_array = RangeArray::from_ranges(sliced, [(0_u32, 2_u32), (1, 2)]).unwrap();

    let via_get = range_array.get(1).unwrap();
    let via_get = via_get.as_any().downcast_ref::<Float64Array>().unwrap();
    let via_get = (0..via_get.len())
        .map(|index| via_get.value(index))
        .collect::<Vec<_>>();
    assert2::assert!(via_get == vec![3.0, 4.0]);
    assert2::assert!(range_array.value_slice(1).unwrap() == via_get.as_slice());
}
