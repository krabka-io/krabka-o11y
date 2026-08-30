use super::*;

pub(crate) fn event_ref(span: &Span, event: &EventRecord) -> krabka_traceql::EventRef {
    krabka_traceql::EventRef {
        time_since_start: Time::from_nanos(event.time_unix_nano.saturating_sub(span.start_ns)),
        name: event.name.clone(),
        attributes: event
            .attrs
            .iter()
            .filter_map(|attr| traceql_attr(attr).map(|value| (attr.key.clone(), value)))
            .collect(),
    }
}
