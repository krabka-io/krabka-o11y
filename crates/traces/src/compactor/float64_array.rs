use super::{MetadataValueArray, Float64Array};

impl MetadataValueArray for Float64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}
