use super::*;

pub(crate) fn insert_attributes(labels: &mut Labels, attributes: &[KeyValue]) {
    for attribute in attributes {
        if let Some(value) = attribute_value(attribute.value.as_ref()) {
            labels.insert(
                normalize_name(&attribute.key, TranslationStrategy::default()),
                value,
            );
        }
    }
}
