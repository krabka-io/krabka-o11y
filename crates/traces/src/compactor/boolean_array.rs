use super::{MetadataValueArray, BooleanArray};

impl MetadataValueArray for BooleanArray {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}
