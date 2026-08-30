use super::{DecodedSeries, DeltaAccumulator, nanos_to_millis};

pub(crate) fn accumulate_delta_float_series(
    series: &mut [DecodedSeries],
    start_time_unix_nano: u64,
    accumulator: &mut DeltaAccumulator,
) {
    for series in series {
        for sample in &mut series.samples {
            sample.value =
                accumulator.accumulate_sum(&series.labels, start_time_unix_nano, sample.value);
            if start_time_unix_nano != 0 {
                sample.start_timestamp_ms = Some(nanos_to_millis(start_time_unix_nano));
            }
        }
    }
}
