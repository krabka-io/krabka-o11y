use super::*;

impl MetadataValueArray for StringArray {
    fn string_value(&self, idx: usize) -> String {
        self.value(idx).to_string()
    }
}
