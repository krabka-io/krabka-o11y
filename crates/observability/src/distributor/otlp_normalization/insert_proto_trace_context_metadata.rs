use super::{Labels, encode_lower_hex};

pub(crate) fn insert_proto_trace_context_metadata(metadata: &mut Labels, name: &str, value: &[u8]) {
    if !value.is_empty() {
        metadata.insert(name.to_string(), encode_lower_hex(value));
    }
}
