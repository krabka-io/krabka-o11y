use super::{MetadataValueArray, Int64Array};

impl MetadataValueArray for Int64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}
