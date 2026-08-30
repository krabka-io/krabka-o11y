use super::{Labels, hex_string};

pub(crate) fn insert_proto_trace_context_metadata(metadata: &mut Labels, name: &str, value: &[u8]) {
    if !value.is_empty() {
        metadata.insert(name.to_string(), hex_string(value));
    }
}
