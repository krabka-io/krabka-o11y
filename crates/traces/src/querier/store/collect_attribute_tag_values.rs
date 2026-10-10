use super::{
    BTreeSet, INSTRUMENTATION_ATTR_PREFIX, RESOURCE_ATTR_PREFIX, RecordBatch, TraceqlError,
    attr_values_with_resource, event_values, link_values, typed_value_parts,
};

/// The tag a tag-values request names, and the key the trace index holds it
/// under.
#[derive(Clone, Copy)]
pub(crate) struct RequestedTag<'a> {
    pub(crate) tag: &'a str,
    pub(crate) index_tag: &'a str,
}

pub(crate) fn collect_attribute_tag_values(
    batch: &RecordBatch,
    requested: RequestedTag<'_>,
    values: &mut BTreeSet<(String, String)>,
) -> Result<(), TraceqlError> {
    let RequestedTag { tag, index_tag } = requested;
    for row in 0..batch.num_rows() {
        for (key, value) in attr_values_with_resource(batch, row, true)? {
            let matches = if let Some(key) = key.strip_prefix(RESOURCE_ATTR_PREFIX) {
                [tag, index_tag].contains(&key)
            } else if let Some(key) = key.strip_prefix(INSTRUMENTATION_ATTR_PREFIX) {
                let requested = tag.strip_prefix("instrumentation.").unwrap_or(tag);
                let indexed = index_tag
                    .strip_prefix(INSTRUMENTATION_ATTR_PREFIX)
                    .unwrap_or(requested);
                [requested, indexed].contains(&key)
            } else {
                [tag, index_tag].contains(&key.as_str())
            };
            if matches {
                values.extend(typed_value_parts(&value));
            }
        }
        for event in event_values(batch, row)? {
            for (key, value) in event.attributes {
                if key == tag || key == index_tag {
                    values.extend(typed_value_parts(&value));
                }
            }
        }
        for link in link_values(batch, row)? {
            for (key, value) in link.attributes {
                if key == tag || key == index_tag {
                    values.extend(typed_value_parts(&value));
                }
            }
        }
    }
    Ok(())
}
