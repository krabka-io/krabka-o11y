use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
};

use super::{AttrValue, EVENT_ATTR_PREFIX, EventRef, InputSpan, LINK_ATTR_PREFIX, LinkRef};

pub(crate) fn raw_attribute_columns(
    spans: &[(&InputSpan, Option<&EventRef>, Option<&LinkRef>)],
    first_values: (bool, bool),
) -> Vec<(&'static str, ArrayRef)> {
    let mut keys = ListBuilder::new(StringBuilder::new());
    let mut unsupported = ListBuilder::new(StringBuilder::new());
    let mut is_array = ListBuilder::new(BooleanBuilder::new());
    let mut strings = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
    let mut ints = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut floats = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
    let mut bools = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
    for (span, event, link) in spans {
        let events = if first_values.0 {
            span.events.iter().collect::<Vec<_>>()
        } else {
            event.iter().copied().collect()
        };
        let links = if first_values.1 {
            span.links.iter().collect::<Vec<_>>()
        } else {
            link.iter().copied().collect()
        };
        let mut event_attrs = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
        for event in events {
            let mut this_event = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
            for (key, value) in &event.attributes {
                this_event.entry(key.clone()).or_default().push(value);
            }
            for (key, values) in this_event {
                event_attrs.entry(key).or_insert(values);
            }
        }
        let mut link_attrs = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
        for link in links {
            let mut this_link = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
            for (key, value) in &link.attributes {
                this_link.entry(key.clone()).or_default().push(value);
            }
            for (key, values) in this_link {
                link_attrs.entry(key).or_insert(values);
            }
        }
        let attrs = span
            .attrs
            .iter()
            .map(|(key, value)| (key.clone(), value))
            .chain(event_attrs.into_iter().flat_map(|(key, values)| {
                values
                    .into_iter()
                    .map(move |value| (format!("{EVENT_ATTR_PREFIX}{key}"), value))
            }))
            .chain(link_attrs.into_iter().flat_map(|(key, values)| {
                values
                    .into_iter()
                    .map(move |value| (format!("{LINK_ATTR_PREFIX}{key}"), value))
            }));
        for (key, value) in attrs {
            keys.values().append_value(&key);
            unsupported
                .values()
                .append_option(if let AttrValue::Unsupported(value) = value {
                    Some(value)
                } else {
                    None
                });
            is_array
                .values()
                .append_value(matches!(value, AttrValue::Array(_)));
            let values = match value {
                AttrValue::Array(values) => values.as_slice(),
                value => std::slice::from_ref(value),
            };
            for value in values {
                if let AttrValue::Str(value) = value {
                    strings.values().values().append_value(value);
                }
                if let AttrValue::Int(value) = value {
                    ints.values().values().append_value(*value);
                }
                if let AttrValue::Float(value) = value {
                    floats.values().values().append_value(*value);
                }
                if let AttrValue::Bool(value) = value {
                    bools.values().values().append_value(*value);
                }
            }
            strings.values().append(true);
            ints.values().append(true);
            floats.values().append(true);
            bools.values().append(true);
        }
        keys.append(true);
        is_array.append(true);
        unsupported.append(true);
        strings.append(true);
        ints.append(true);
        floats.append(true);
        bools.append(true);
    }
    vec![
        ("attr_keys", Arc::new(keys.finish())),
        ("attr_is_array", Arc::new(is_array.finish())),
        ("attr_value_unsupported", Arc::new(unsupported.finish())),
        ("attr_value", Arc::new(strings.finish())),
        ("attr_value_int", Arc::new(ints.finish())),
        ("attr_value_double", Arc::new(floats.finish())),
        ("attr_value_bool", Arc::new(bools.finish())),
    ]
}
