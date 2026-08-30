use super::{RecordBatch, ColdAttributeTagNames, TraceqlError, attr_values_with_resource, RESOURCE_ATTR_PREFIX, INSTRUMENTATION_ATTR_PREFIX, event_values, link_values};

pub(crate) fn collect_attribute_tag_names(
    batch: &RecordBatch,
    names: &mut ColdAttributeTagNames,
) -> Result<(), TraceqlError> {
    for row in 0..batch.num_rows() {
        for (key, _) in attr_values_with_resource(batch, row, true)? {
            if let Some(key) = key.strip_prefix(RESOURCE_ATTR_PREFIX) {
                names.resource.insert(key.to_string());
            } else if let Some(key) = key.strip_prefix(INSTRUMENTATION_ATTR_PREFIX) {
                names.instrumentation.insert(key.to_string());
            } else {
                names.span.insert(key);
            }
        }
        for event in event_values(batch, row)? {
            names
                .event
                .extend(event.attributes.into_iter().map(|(key, _)| key));
        }
        for link in link_values(batch, row)? {
            names
                .link
                .extend(link.attributes.into_iter().map(|(key, _)| key));
        }
    }
    Ok(())
}
