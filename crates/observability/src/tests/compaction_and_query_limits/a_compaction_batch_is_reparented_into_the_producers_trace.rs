use super::*;

#[path = "../../../tests/support/recording_trace_subscriber.rs"]
mod recording_trace_subscriber;

/// The compaction span belongs to the producer's trace, taken from the
/// first record that actually carries a `traceparent`. A record without
/// one sits first on purpose: selecting it instead extracts no context and
/// leaves the batch in a trace of its own.
#[test]
pub(crate) fn a_compaction_batch_is_reparented_into_the_producers_trace() {
    use opentelemetry::trace::{TraceContextExt as _, TraceId};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;

    recording_trace_subscriber::with_recording_trace("observability-compaction-test", || {
        let record = |key: &str, value: &str| KafkaWalRecord {
            value: Vec::new(),
            partition: PartitionIndex(0),
            offset: Offset(0),
            timestamp_ms: None,
            headers: vec![KafkaWalHeader {
                key: key.to_owned(),
                value: Some(value.as_bytes().to_vec()),
            }],
        };
        let records = vec![
            record("tenant", "tenant-a"),
            record(
                "traceparent",
                "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            ),
        ];

        let span = tracing::info_span!("logs_compaction");
        set_remote_parent_from_wal_records(&span, &records);

        let context = span.context().span().span_context().clone();
        check!(
            context.trace_id()
                == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").expect("a trace id")
        );
    });
}
