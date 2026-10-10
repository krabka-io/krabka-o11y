use super::{InstrumentationGroups, SpanRef, instrumentation_attributes};

/// Group spans by their instrumentation scope's name, version, and
/// attributes, in the order each scope first appears.
pub(crate) fn group_by_instrumentation(input_spans: Vec<&SpanRef>) -> InstrumentationGroups<'_> {
    let mut groups: InstrumentationGroups<'_> = Vec::new();
    for span in input_spans {
        let key = (
            span.instrumentation_name.clone(),
            span.instrumentation_version.clone(),
            instrumentation_attributes(span),
        );
        if let Some((_, spans)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            spans.push(span);
        } else {
            groups.push((key, vec![span]));
        }
    }
    groups
}
