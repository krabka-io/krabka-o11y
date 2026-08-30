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

    match (base.is_empty(), index_prefix.is_empty()) {
        (true, true) => ObjectPath::from(""),
        (true, false) => ObjectPath::from(index_prefix),
        (false, true) => ObjectPath::from(base),
        (false, false) => ObjectPath::from(format!("{base}/{index_prefix}")),
    }
}
