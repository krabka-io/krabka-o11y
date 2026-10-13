use super::{BTreeSet, Span, typed_value_parts};

pub(crate) fn collect_event_values(
    span: &Span,
    tag: &str,
    values: &mut BTreeSet<krabka_traceql::TypedValue>,
) {
    for event in &span.events {
        match tag {
            "event:name" => {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    event.name.clone(),
                ));
            }
            "event:timeSinceStart" => {
                values.insert(krabka_traceql::TypedValue::new(
                    "duration",
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
                        .flat_map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
    }
}
