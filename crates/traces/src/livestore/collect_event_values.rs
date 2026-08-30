use super::*;

pub(crate) fn collect_event_values(span: &Span, tag: &str, values: &mut BTreeSet<(String, String)>) {
    for event in &span.events {
        match tag {
            "event:name" => {
                values.insert(("string".into(), event.name.clone()));
            }
            "event:timeSinceStart" => {
                values.insert((
                    "duration".into(),
                    event
                        .time_unix_nano
                        .saturating_sub(span.start_ns)
                        .to_string(),
                ));
            }
            _ => {
                values.extend(
                    event
                        .attrs
                        .iter()
                        .filter(|attr| attr.key == tag)
                        .map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
    }
}
