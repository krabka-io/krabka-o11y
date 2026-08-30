use super::*;

/// Per-range-query float-scan cache (see `PromqlEngine::scan_float_rows`).
///
/// A range query evaluates the same selector at every step, and each step's
/// instant scan covers `[step - lookback, step]`. Those windows overlap almost
/// completely, so a driver without a cache re-scans the store once per step
/// (240x for a 1h/15s query).
///
/// This cache scans the union window `[start - lookback, end]` one time per
/// matcher set and serves each step from the in-memory result. The store is a
/// pure time-range filter, so a filtered superset is byte-for-byte what a direct
/// sub-window scan returns. Both stores keep `[start, end]` inclusive.
///
/// Only requests inside the pre-scanned union use the cache. An
/// `offset`-modified, `@`-modified, or long-`[range]` scan outside the union
/// falls back to a direct scan. Results therefore never change, and only the
/// redundant re-scans are removed.
pub(crate) struct RangeScanCacheInner {
    pub(crate) full_start_ms: i64,
    pub(crate) full_end_ms: i64,
    pub(crate) floats: HashMap<String, Arc<Vec<FloatRow>>>,
    /// Per-matcher-set histogram rows over the union window. The instant-selector
    /// path probes for histogram series at every step
    /// (`selector_has_histogram_series`). This probe is a second per-step store
    /// scan next to the float scan, so the cache holds it the same way.
    pub(crate) histograms: HashMap<String, Arc<Vec<HistogramRow>>>,
    /// Per-matcher-set fingerprint->labels resolution. A series label set is
    /// immutable, so the union-window result is a superset of the active series
    /// of any sub-window. Callers use it only as a `get(&fp)` lookup keyed by
    /// rows already filtered to the sub-window, so they never read the extra
    /// entries.
    pub(crate) labels: HashMap<String, Arc<BTreeMap<SeriesFingerprint, Labels>>>,
}
