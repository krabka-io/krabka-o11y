use super::{BTreeMap, Map, Value};

pub(crate) fn labels_json<'a, V: AsRef<str> + 'a>(
    labels: impl Iterator<Item = (&'a String, &'a V)>,
) -> Value {
    let pairs = labels
        .filter(|(name, value)| name.as_str() != "__unit__" || !value.as_ref().is_empty())
        .map(|(name, value)| (name.clone(), Value::String(value.as_ref().to_owned())))
        .collect::<BTreeMap<_, _>>();
    Value::Object(Map::from_iter(pairs))
}
