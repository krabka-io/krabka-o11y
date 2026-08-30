use super::{EventRef, StringBuilder, Int64Builder, TimeExt};

pub(crate) fn append_nested_event(
    event: Option<&EventRef>,
    event_name: &mut StringBuilder,
    event_time_since_start: &mut Int64Builder,
) {
    if let Some(event) = event {
        event_name.append_value(&event.name);
        event_time_since_start.append_value(event.time_since_start.nanos_i64());
    } else {
        event_name.append_null();
        event_time_since_start.append_null();
    }
}
