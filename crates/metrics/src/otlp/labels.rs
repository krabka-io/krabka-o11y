use super::{ExtraLabel, KeyValue, Labels, TranslationStrategy, insert_attributes};

pub(crate) fn labels(
    name: &str,
    resource_attributes: &[KeyValue],
    point_attributes: &[KeyValue],
    extra: Option<ExtraLabel<'_>>,
    strategy: TranslationStrategy,
) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    insert_attributes(&mut labels, resource_attributes, strategy);
    insert_attributes(&mut labels, point_attributes, strategy);
    if let Some(extra) = extra {
        labels.insert(extra.label_name, extra.label_value);
    }
    labels
}
