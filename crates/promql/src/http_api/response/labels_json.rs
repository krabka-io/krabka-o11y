use super::{BTreeMap, Labels, Map, Value};

pub(crate) fn labels_json(labels: &Labels) -> Value {
    let pairs = labels
        .iter()
        .filter(|(name, value)| name.as_str() != "__unit__" || !value.is_empty())
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect::<BTreeMap<_, _>>();
    Value::Object(Map::from_iter(pairs))
}
