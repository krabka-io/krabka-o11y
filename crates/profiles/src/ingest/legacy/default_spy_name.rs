/// The `pyroscope_spy` label value for an upload that names no profiler.
///
/// Pyroscope writes the label unconditionally, so a `/ingest` request with no
/// `?spyName=` still produces a series carrying `pyroscope_spy="unknown"`.
pub(crate) const DEFAULT_SPY_NAME: &str = "unknown";
