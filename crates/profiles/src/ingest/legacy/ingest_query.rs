use super::IngestFormat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestQuery {
    pub name: String,
    pub profile_type_suffix: Option<String>,
    pub labels: Vec<(String, String)>,
    pub format: IngestFormat,
    pub sample_rate: u32,
    pub units: String,
    pub from_ms: Option<i64>,
    pub until_ms: Option<i64>,
    /// The `?spyName=` profiler that produced the upload. Pyroscope stores it
    /// as the `pyroscope_spy` series label and defaults it to `unknown`.
    pub spy_name: String,
    /// Async-profiler event mode reported by the JFR producer.
    pub jfr_event: String,
}
