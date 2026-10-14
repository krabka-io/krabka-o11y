use super::{BinaryInput, KeyValue, WireError};

pub(crate) fn read_binary_key_value(input: &mut BinaryInput<'_>) -> Result<KeyValue, WireError> {
    super::parse_key_value(input)
}
