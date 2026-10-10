use super::{Bytes, ProducerHeader};
use crate::persisted_format::{PERSISTED_FORMAT_HEADER, PERSISTED_FORMAT_VERSION};

/// Returns the Kafka headers every signal stamps on a WAL record.
///
/// The persisted-format version header comes first. The current span's W3C
/// trace context (`traceparent`/`tracestate`) follows, so the WAL consumer can
/// re-parent its span onto the ingest trace. The trace headers are additive:
/// there are none when no span is active and sampled.
#[must_use]
pub fn wal_record_headers() -> Vec<ProducerHeader> {
    std::iter::once(ProducerHeader {
        key: PERSISTED_FORMAT_HEADER.to_string(),
        value: Some(Bytes::from_static(PERSISTED_FORMAT_VERSION)),
    })
    .chain(
        krabka_telemetry::propagation::current_trace_headers()
            .into_iter()
            .map(|(key, value)| ProducerHeader {
                key,
                value: Some(Bytes::from(value.into_bytes())),
            }),
    )
    .collect()
}
