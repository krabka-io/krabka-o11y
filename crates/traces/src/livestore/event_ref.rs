use krabka_units::convert::TimeExt as _;

use super::{EventRecord, Span, Time, traceql_attributes};

pub(crate) fn event_ref(span: &Span, event: &EventRecord) -> krabka_traceql::EventRef {
    krabka_traceql::EventRef {
        time_since_start: Time::from_nanos(event.time_unix_nano.saturating_sub(span.start_ns)),
        name: event.name.clone(),
        attributes: traceql_attributes(&event.attrs),
    }
}
