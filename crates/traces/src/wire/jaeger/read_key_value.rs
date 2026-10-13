use super::{CompactInput, KeyValue, WireError};

pub(crate) fn read_key_value(input: &mut CompactInput<'_>) -> Result<KeyValue, WireError> {
    super::parse_key_value(input)
}
