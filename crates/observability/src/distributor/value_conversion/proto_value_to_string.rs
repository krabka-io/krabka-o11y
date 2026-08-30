use super::{ProtoAnyValue, proto_any_value_to_string};

pub(crate) fn proto_value_to_string(value: &ProtoAnyValue) -> String {
    value
        .value
        .as_ref()
        .map(proto_any_value_to_string)
        .unwrap_or_default()
}
