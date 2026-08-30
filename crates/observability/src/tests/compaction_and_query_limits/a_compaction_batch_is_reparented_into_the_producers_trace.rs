use super::*;

/// The compaction span belongs to the producer's trace, taken from the
/// first record that actually carries a `traceparent`. A record without
/// one sits first on purpose: selecting it instead extracts no context and
/// leaves the batch in a trace of its own.
#[test]
pub(crate) fn a_compaction_batch_is_reparented_into_the_producers_trace() {
    use opentelemetry::trace::{TraceContextExt as _, TraceId, TracerProvider as _};
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use tracing_opentelemetry::OpenTelemetrySpanExt as _;
    use tracing_subscriber::layer::SubscriberExt as _;

    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let tracer = provider.tracer("observability-compaction-test");
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

    tracing::subscriber::with_default(subscriber, || {
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
