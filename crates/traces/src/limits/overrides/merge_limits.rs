use super::*;

pub(crate) fn merge_limits(defaults: &Limits, partial: &PartialLimits) -> Limits {
    Limits {
        ingestion_rate: partial
            .ingestion_rate_spans_per_sec
            .map_or(defaults.ingestion_rate, Frequency::from_per_sec),
        ingestion_burst_spans: partial
            .ingestion_burst_spans
            .unwrap_or(defaults.ingestion_burst_spans),
        max_traces_per_search: partial
            .max_traces_per_search
            .unwrap_or(defaults.max_traces_per_search),
        max_spans_per_trace: partial
            .max_spans_per_trace
            .unwrap_or(defaults.max_spans_per_trace),
        max_attribute: partial
            .max_attribute_bytes
            .map_or(defaults.max_attribute, ByteSize::from_bytes),
        max_search_duration: partial
            .max_search_duration_secs
            .map_or(defaults.max_search_duration, |secs| {
                Time::from_secs(i64::try_from(secs).unwrap_or(i64::MAX))
            }),
    }
}
