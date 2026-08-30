#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanJob {
    pub object_key: String,
    pub row_group_start: usize,
    pub row_group_end: usize,
}
