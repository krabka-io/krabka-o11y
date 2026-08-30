use super::{KeyValue, Labels, insert_attributes};

pub(crate) fn labels(
    name: &str,
    resource_attributes: &[KeyValue],
    point_attributes: &[KeyValue],
    extra: Option<(&str, &str)>,
) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    insert_attributes(&mut labels, resource_attributes);
    insert_attributes(&mut labels, point_attributes);
    if let Some((name, value)) = extra {
        labels.insert(name, value);
    }
    labels
}
