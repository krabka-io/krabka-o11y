use super::ObjectPath;

pub(crate) fn effective_object_store_prefix(
    base: Option<&ObjectPath>,
    index_prefix: &str,
) -> ObjectPath {
    let index_prefix = index_prefix.trim_matches('/');
    let Some(base) = base else {
        return ObjectPath::from(index_prefix);
    };
    let base = base.as_ref().trim_matches('/');

    if base.is_empty() {
        if index_prefix.is_empty() {
            ObjectPath::from("")
        } else {
            ObjectPath::from(index_prefix)
        }
    } else if index_prefix.is_empty() {
        ObjectPath::from(base)
    } else {
        ObjectPath::from(format!("{base}/{index_prefix}"))
    }
}
