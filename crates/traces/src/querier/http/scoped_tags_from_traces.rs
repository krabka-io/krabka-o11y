use super::{BTreeSet, ScopedTag, TagScope, TraceSpans, trace_resource_attributes};

pub(crate) fn scoped_tags_from_traces(
    traces: &[TraceSpans],
    scope: Option<TagScope>,
) -> Vec<ScopedTag> {
    let mut out = Vec::new();

    if matches!(scope, None | Some(TagScope::Resource)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            tags.extend(
                trace_resource_attributes(trace)
                    .into_iter()
                    .map(|(key, _)| key),
            );
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Resource,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Span)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                tags.extend(span.attributes.iter().map(|(key, _)| key.clone()));
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Span,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Event)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                for event in &span.events {
                    tags.extend(event.attributes.iter().map(|(key, _)| key.clone()));
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Event,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Link)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                for link in &span.links {
                    tags.extend(link.attributes.iter().map(|(key, _)| key.clone()));
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Link,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Instrumentation)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                if !span.instrumentation_name.is_empty() {
                    tags.insert("instrumentation:name".to_string());
                }
                if !span.instrumentation_version.is_empty() {
                    tags.insert("instrumentation:version".to_string());
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Instrumentation,
                tags: tags.into_iter().collect(),
            });
        }
    }

    out
}
