use super::{MetricValue, Value, parse_metric_sample_value};

/// The value slot of a `[timestamp, "value"]` sample, and the value it holds.
pub(crate) struct SampleValueSlot<'a> {
    pub(crate) slot: &'a mut Value,
    pub(crate) sample_value: MetricValue,
}

/// Finds the value slot of `sample`, when the sample is an array whose second
/// element is a decimal string.
pub(crate) fn sample_value_slot(sample: &mut Value) -> Option<SampleValueSlot<'_>> {
    let slot = sample.as_array_mut()?.get_mut(1)?;
    let sample_value = slot.as_str().and_then(parse_metric_sample_value)?;
    Some(SampleValueSlot { slot, sample_value })
}
