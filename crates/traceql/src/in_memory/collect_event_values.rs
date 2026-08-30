use super::*;

pub(crate) fn collect_event_values(span: &InputSpan, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for event in &span.events {
        match tag {
            "event:name" => {
                values.insert(("string".to_string(), event.name.clone()));
            }
            "event:timeSinceStart" => {
                values.insert((
                    "duration".to_string(),
                    event.time_since_start.nanos_i64().to_string(),
                ));
            }
            _ => {}
        }
        values.extend(
            event
                .attributes
                .iter()
                .filter(|(key, _)| nested_attribute_key_matches(key, tag, "event."))
                .map(|(_, value)| typed_value_parts(value)),
        );
    }
}
