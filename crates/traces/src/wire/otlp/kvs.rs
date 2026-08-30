use super::*;

pub(crate) fn kvs(attrs: &[OtlpKv]) -> Vec<KeyValue> {
    attrs.iter().flat_map(kv_to_attrs).collect()
}
