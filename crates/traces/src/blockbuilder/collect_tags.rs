use super::{Span, BTreeSet, BTreeMap, attr_value_string, insert_tag_value};

pub(crate) fn collect_tags(
    spans: &[Span],
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) {
    for span in spans {
        for attr in span.resource_attrs.iter().chain(&span.span_attrs) {
            tag_names.insert(attr.key.clone());
            tag_values
                .entry(attr.key.clone())
                .or_default()
                .insert(attr_value_string(&attr.value));
        }
        for event in &span.events {
            insert_tag_value(tag_names, tag_values, "event:name", event.name.clone());
            insert_tag_value(
                tag_names,
                tag_values,
                "event:timeSinceStart",
                event
                    .time_unix_nano
                    .saturating_sub(span.start_ns)
                    .to_string(),
            );
            for attr in &event.attrs {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    &attr.key,
                    attr_value_string(&attr.value),
                );
            }
        }
        for link in &span.links {
            insert_tag_value(
                tag_names,
                tag_values,
                "link:traceID",
                hex::encode(link.trace_id),
            );
            insert_tag_value(
                tag_names,
                tag_values,
                "link:spanID",
                hex::encode(link.span_id),
            );
            for attr in &link.attrs {
                insert_tag_value(
                    tag_names,
                    tag_values,
                    &attr.key,
                    attr_value_string(&attr.value),
                );
            }
        }
        if !span.instrumentation_scope.is_empty() {
            insert_tag_value(
                tag_names,
                tag_values,
                "instrumentation:name",
                span.instrumentation_scope.clone(),
            );
        }
        if !span.instrumentation_version.is_empty() {
            insert_tag_value(
                tag_names,
                tag_values,
                "instrumentation:version",
                span.instrumentation_version.clone(),
            );
        }
    }
}
