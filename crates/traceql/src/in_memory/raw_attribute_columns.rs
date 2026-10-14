use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
};

use super::{AttrValue, EVENT_ATTR_PREFIX, EventRef, InputSpan, LINK_ATTR_PREFIX, LinkRef};

pub(crate) struct SpanEventLink<'a> {
    pub(crate) span: &'a InputSpan,
    pub(crate) event: Option<&'a EventRef>,
    pub(crate) link: Option<&'a LinkRef>,
}

#[derive(Clone, Copy)]
pub(crate) struct RawAttributeSelection {
    pub(crate) all_events: bool,
    pub(crate) all_links: bool,
}

pub(crate) struct AttributeArray {
    pub(crate) name: &'static str,
    pub(crate) array: ArrayRef,
}

pub(crate) fn raw_attribute_columns(
    spans: &[SpanEventLink<'_>],
    selection: RawAttributeSelection,
) -> Vec<AttributeArray> {
    let mut keys = ListBuilder::new(StringBuilder::new());
    let mut unsupported = ListBuilder::new(StringBuilder::new());
    let mut is_array = ListBuilder::new(BooleanBuilder::new());
    let mut strings = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
    let mut ints = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut floats = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
    let mut bools = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
    for item in spans {
        let events = if selection.all_events {
            item.span.events.iter().collect::<Vec<_>>()
        } else {
            item.event.iter().copied().collect()
        };
        let links = if selection.all_links {
            item.span.links.iter().collect::<Vec<_>>()
        } else {
            item.link.iter().copied().collect()
        };
        let mut event_attrs = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
        for event in events {
            let mut this_event = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
            for attribute in &event.attributes {
                this_event
                    .entry(attribute.key.clone())
                    .or_default()
                    .push(&attribute.value);
            }
            for entry in this_event {
                event_attrs.entry(entry.0).or_insert(entry.1);
            }
        }
        let mut link_attrs = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
        for link in links {
            let mut this_link = std::collections::BTreeMap::<String, Vec<&AttrValue>>::new();
            for attribute in &link.attributes {
                this_link
                    .entry(attribute.key.clone())
                    .or_default()
                    .push(&attribute.value);
            }
            for entry in this_link {
                link_attrs.entry(entry.0).or_insert(entry.1);
            }
        }
        let attrs = item
            .span
            .attrs
            .iter()
            .map(|attribute| RawAttribute {
                key: attribute.key.clone(),
                value: &attribute.value,
            })
            .chain(event_attrs.into_iter().flat_map(|entry| {
                let key = entry.0;
                entry.1.into_iter().map(move |value| RawAttribute {
                    key: format!("{EVENT_ATTR_PREFIX}{key}"),
                    value,
                })
            }))
            .chain(link_attrs.into_iter().flat_map(|entry| {
                let key = entry.0;
                entry.1.into_iter().map(move |value| RawAttribute {
                    key: format!("{LINK_ATTR_PREFIX}{key}"),
                    value,
                })
            }));
        for attribute in attrs {
            let key = attribute.key;
            let value = attribute.value;
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
        AttributeArray {
            name: "attr_keys",
            array: Arc::new(keys.finish()),
        },
        AttributeArray {
            name: "attr_is_array",
            array: Arc::new(is_array.finish()),
        },
        AttributeArray {
            name: "attr_value_unsupported",
            array: Arc::new(unsupported.finish()),
        },
        AttributeArray {
            name: "attr_value",
            array: Arc::new(strings.finish()),
        },
        AttributeArray {
            name: "attr_value_int",
            array: Arc::new(ints.finish()),
        },
        AttributeArray {
            name: "attr_value_double",
            array: Arc::new(floats.finish()),
        },
        AttributeArray {
            name: "attr_value_bool",
            array: Arc::new(bools.finish()),
        },
    ]
}

struct RawAttribute<'a> {
    key: String,
    value: &'a AttrValue,
}
