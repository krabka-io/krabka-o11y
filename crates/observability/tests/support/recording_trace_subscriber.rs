use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_subscriber::prelude::*;

pub fn with_recording_trace<T>(name: &'static str, operation: impl FnOnce() -> T) -> T {
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    let provider = SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .build();
    let tracer = provider.tracer(name);
    let subscriber =
        tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
    tracing::subscriber::with_default(subscriber, operation)
}
