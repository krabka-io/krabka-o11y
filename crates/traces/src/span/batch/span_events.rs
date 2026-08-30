use super::*;

pub(crate) fn span_events(span: &Span) -> Vec<SpanEvent> {
    span.events
        .iter()
        .map(|event| SpanEvent {
            name: event.name.clone(),
            time_since_start: Time::from_nanos(event.time_unix_nano.saturating_sub(span.start_ns)),
            attrs: event_attrs(&event.attrs),
        })
        .collect()
}
