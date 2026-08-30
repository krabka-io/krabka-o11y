use super::MappingRec;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawLocation {
    pub address: u64,
    pub mapping: MappingRec,
    pub filename: String,
    pub build_id: String,
}
