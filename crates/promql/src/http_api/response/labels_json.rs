use super::{Labels, Value, BTreeMap, Map};

pub(crate) fn labels_json(labels: &Labels) -> Value {
    let pairs = labels
        .iter()
        .map(|(name, value)| (name.clone(), Value::String(value.clone())))
        .collect::<BTreeMap<_, _>>();
    Value::Object(Map::from_iter(pairs))
}
