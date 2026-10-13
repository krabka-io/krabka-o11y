use super::{
    BTreeSet, EventRef, TimeExt as _, TypedValue, nested_attribute_key_matches, typed_value_parts,
};

/// Adds the `(type, value)` pairs that `tag` lists across `events`.
///
/// `tag` is an event intrinsic, an event attribute, or an attribute key
/// with the `event.` scope prefix.
pub fn collect_event_values(events: &[EventRef], tag: &str, values: &mut BTreeSet<TypedValue>) {
    for event in events {
        match tag {
            "event:name" => {
                values.insert(TypedValue {
                    type_: "string".to_string(),
                    value: event.name.clone(),
                });
            }
            "event:timeSinceStart" => {
                values.insert(TypedValue {
                    type_: "duration".to_string(),
                    value: event.time_since_start.nanos_i64().to_string(),
                });
            }
            _ => {}
        }
        values.extend(
            event
                .attributes
                .iter()
                .filter(|attribute| nested_attribute_key_matches(&attribute.key, tag, "event."))
                .flat_map(|attribute| typed_value_parts(&attribute.value)),
        );
    }
}
