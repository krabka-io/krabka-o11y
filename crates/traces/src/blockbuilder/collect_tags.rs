use super::{BTreeMap, BTreeSet, Span, TagCatalog, attr_value_string};

pub(crate) fn collect_tags(
    spans: &[Span],
    tag_names: &mut BTreeSet<String>,
    tag_values: &mut BTreeMap<String, BTreeSet<String>>,
) {
    let mut catalog = TagCatalog {
        names: tag_names,
        values: tag_values,
    };
    for span in spans {
        for attr in span.resource_attrs.iter().chain(&span.span_attrs) {
            catalog.insert(&attr.key, attr_value_string(&attr.value));
        }
        for event in &span.events {
            catalog.insert("event:name", event.name.clone());
            catalog.insert(
                "event:timeSinceStart",
                event
                    .time_unix_nano
                    .saturating_sub(span.start_ns)
                    .to_string(),
            );
            for attr in &event.attrs {
                catalog.insert(&attr.key, attr_value_string(&attr.value));
            }
        }
        for link in &span.links {
            catalog.insert("link:traceID", hex::encode(link.trace_id));
            catalog.insert("link:spanID", hex::encode(link.span_id));
            for attr in &link.attrs {
                catalog.insert(&attr.key, attr_value_string(&attr.value));
            }
        }
        if !span.instrumentation_scope.is_empty() {
            catalog.insert("instrumentation:name", span.instrumentation_scope.clone());
        }
        if !span.instrumentation_version.is_empty() {
            catalog.insert(
                "instrumentation:version",
                span.instrumentation_version.clone(),
            );
        }
    }
}
