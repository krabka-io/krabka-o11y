use super::*;

pub(crate) fn samples_to_proto(row: &WireTimeSeries) -> Vec<Sample> {
    if row.native_histogram.is_some() {
        Vec::new()
    } else {
        vec![Sample {
            value: row.value,
            timestamp: row.timestamp_ms,
        }]
    }
}
