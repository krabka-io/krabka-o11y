use super::{Float64Array, MetadataValueArray};

impl MetadataValueArray for Float64Array {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}
