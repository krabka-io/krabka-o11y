use super::*;

pub(crate) fn append_events(events: &mut ListBuilder<StructBuilder>, rows: &[SpanEvent]) {
    let sb = events.values();
    for event in rows {
        sb.field_builder::<StringBuilder>(0)
            .expect("event name builder")
            .append_value(&event.name);
        sb.field_builder::<Int64Builder>(1)
            .expect("event time builder")
            .append_value(event.time_since_start.nanos_i64());
        append_kv(sb, &event.attrs);
        sb.append(true);
    }
    events.append(true);
}
