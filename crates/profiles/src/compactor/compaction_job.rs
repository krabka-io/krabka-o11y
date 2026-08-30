use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionJob {
    pub tenant: String,
    pub input_keys: Vec<String>,
    pub output_key: String,
}
