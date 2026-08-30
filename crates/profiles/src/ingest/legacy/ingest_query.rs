use super::IngestFormat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestQuery {
    pub name: String,
    pub labels: Vec<(String, String)>,
    pub format: IngestFormat,
    pub sample_rate: u32,
    pub units: String,
    pub from_ms: Option<i64>,
    pub until_ms: Option<i64>,
}
