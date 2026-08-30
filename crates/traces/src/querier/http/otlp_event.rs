use super::{SpanRef, OtlpEvent, event_unix_nano, otlp_attrs};

pub(crate) fn otlp_event(span: &SpanRef, event: &krabka_traceql::EventRef) -> OtlpEvent {
    OtlpEvent {
        time_unix_nano: event_unix_nano(span, event),
        name: event.name.clone(),
        attributes: otlp_attrs(&event.attributes),
        ..OtlpEvent::default()
    }
}
