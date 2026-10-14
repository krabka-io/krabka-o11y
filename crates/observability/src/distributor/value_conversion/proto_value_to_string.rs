use super::{ProtoAnyValue, proto_any_value_to_string};

pub(crate) fn proto_value_to_string(value: &ProtoAnyValue) -> String {
    value
        .value
        .as_ref()
        .map_or_else(Default::default, proto_any_value_to_string)
}
