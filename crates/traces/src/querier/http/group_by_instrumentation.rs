use super::{
    InstrumentationGroup, InstrumentationGroups, InstrumentationKey, SpanRef,
    instrumentation_attributes,
};

/// Group spans by their instrumentation scope's name, version, and
/// attributes, in the order each scope first appears.
pub(crate) fn group_by_instrumentation(input_spans: Vec<&SpanRef>) -> InstrumentationGroups<'_> {
    let mut groups: InstrumentationGroups<'_> = Vec::new();
    for span in input_spans {
        let key = InstrumentationKey {
            name: span.instrumentation_name.clone(),
            version: span.instrumentation_version.clone(),
            attributes: instrumentation_attributes(span),
        };
        if let Some(group) = groups.iter_mut().find(|group| group.key == key) {
            group.spans.push(span);
        } else {
            groups.push(InstrumentationGroup {
                key,
                spans: vec![span],
            });
        }
    }
    groups
}
