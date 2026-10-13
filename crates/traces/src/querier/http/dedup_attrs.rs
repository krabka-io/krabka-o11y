use super::{AttrValue, Attribute, BTreeSet};

pub(crate) fn dedup_attrs(attrs_in: &[Attribute], fallback_service_name: &str) -> Vec<Attribute> {
    let mut seen = BTreeSet::new();
    let mut attrs = Vec::new();
    for attribute in attrs_in {
        seen.insert(attribute.key.clone());
        attrs.push(attribute.clone());
    }
    if !fallback_service_name.is_empty() && seen.insert("service.name".into()) {
        attrs.push(Attribute::new(
            "service.name",
            AttrValue::Str(fallback_service_name.to_string()),
        ));
    }
    attrs
}
