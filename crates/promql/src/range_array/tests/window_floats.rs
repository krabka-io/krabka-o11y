use super::*;

/// The floats of `range_array`'s window at `index`.
pub(crate) fn window_floats(range_array: &RangeArray, index: usize) -> Vec<f64> {
    let window = range_array.get(index).unwrap();
    let window = window.as_any().downcast_ref::<Float64Array>().unwrap();
    (0..window.len()).map(|i| window.value(i)).collect()
}
