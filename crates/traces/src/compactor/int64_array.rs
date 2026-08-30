use super::{Int64Array, MetadataValueArray};

impl MetadataValueArray for Int64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}
