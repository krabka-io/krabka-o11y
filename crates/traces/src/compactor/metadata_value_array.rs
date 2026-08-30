use super::Array;

pub(crate) trait MetadataValueArray: Array {
    fn string_value(&self, idx: usize) -> String;
}
