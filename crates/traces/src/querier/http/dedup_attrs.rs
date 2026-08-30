use super::*;

pub(crate) fn dedup_attrs(
    attrs_in: &[(String, AttrValue)],
    fallback_service_name: &str,
) -> Vec<(String, AttrValue)> {
    let mut seen = BTreeSet::new();
    let mut attrs = Vec::new();
    for (key, value) in attrs_in {
        seen.insert(key.clone());
        attrs.push((key.clone(), value.clone()));
    }
    if !fallback_service_name.is_empty() && seen.insert("service.name".into()) {
        attrs.push((
            "service.name".into(),
            AttrValue::Str(fallback_service_name.to_string()),
        ));
    }
    attrs
}
