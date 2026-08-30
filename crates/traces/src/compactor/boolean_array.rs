use super::{BooleanArray, MetadataValueArray};

impl MetadataValueArray for BooleanArray {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}
