use krabka_blockstore::LabelPairRef;
use krabka_metrics::MetricLabelRef;

use super::{BTreeMap, Map, Value};

pub(crate) trait LabelJsonPair {
    fn name(&self) -> &str;
    fn value(&self) -> &str;
}

impl LabelJsonPair for LabelPairRef<'_> {
    fn name(&self) -> &str {
        self.name
    }
    fn value(&self) -> &str {
        self.value
    }
}

impl LabelJsonPair for MetricLabelRef<'_> {
    fn name(&self) -> &str {
        self.name
    }
    fn value(&self) -> &str {
        self.value.as_str()
    }
}

pub(crate) fn labels_json(labels: impl Iterator<Item = impl LabelJsonPair>) -> Value {
    let mut pairs = BTreeMap::new();
    for pair in labels {
        if pair.name() != "__unit__" || !pair.value().is_empty() {
            pairs.insert(
                pair.name().to_owned(),
                Value::String(pair.value().to_owned()),
            );
        }
    }
    Value::Object(Map::from_iter(pairs))
}
