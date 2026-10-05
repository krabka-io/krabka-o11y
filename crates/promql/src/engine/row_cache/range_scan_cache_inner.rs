use super::{Arc, BTreeMap, HashMap, Labels, ScannedRows, SeriesFingerprint, Windowed};

/// Per-query store-scan cache (see `PromqlEngine::scanned_rows`).
///
/// A range query evaluates the same selector at every step, and each step's
/// instant scan covers `[step - lookback, step]`. Those windows overlap almost
/// completely, so a driver without a cache re-scans the store once per step
/// (240x for a 1h/15s query). The range driver sets `union` to
/// `[start - lookback, end]`, and each matcher set scans that window one time.
///
/// An instant query sets `union` to `None`. Then each request that the cache
/// cannot serve scans its own window and keeps the result in place of the
/// previous one for that matcher set. The histogram probe of a selector asks
/// for the whole window of the selector, so the float scan of the same
/// selector that follows it reads the cache and not the store.
///
/// One store scan fills the float rows and the histogram rows of a window
/// together, because the store registers both tables in the same scan.
///
/// The store is a pure time-range filter, so a filtered superset is
/// byte-for-byte what a direct sub-window scan returns. Both stores keep
/// `[start, end]` inclusive. Inside a range query, a request outside the union
/// (an `offset`-modified, `@`-modified, or long-`[range]` scan) reads the store
/// directly and is not kept. Results therefore never change, and only the
/// redundant store scans are removed.
pub(crate) struct RangeScanCacheInner {
    /// A single instant leaf can use latest samples without changing the
    /// shared full-scan snapshot seen by composite or range expressions.
    pub(crate) allow_latest_float_samples: bool,
    /// The window that a range query scans for each matcher set, or `None`
    /// for an instant query.
    pub(crate) union: Option<(i64, i64)>,
    /// Per-matcher-set float and histogram rows.
    pub(crate) rows: HashMap<String, Windowed<ScannedRows>>,
    /// Per-matcher-set fingerprint->labels resolution. A series label set is
    /// immutable, so the result for a wider window is a superset of the active
    /// series of any sub-window. Callers use it only as a `get(&fp)` lookup
    /// keyed by rows already filtered to the sub-window, so they never read the
    /// extra entries.
    pub(crate) labels: HashMap<String, Windowed<BTreeMap<SeriesFingerprint, Arc<Labels>>>>,
}

impl RangeScanCacheInner {
    /// A cache for a range query that scans `[start_ms, end_ms]` for each
    /// matcher set.
    pub(crate) fn range(start_ms: i64, end_ms: i64) -> Self {
        Self::new(Some((start_ms, end_ms)))
    }

    /// A cache for an instant query, which keeps the window of each scan.
    pub(crate) fn instant() -> Self {
        Self::new(None)
    }

    fn new(union: Option<(i64, i64)>) -> Self {
        Self {
            allow_latest_float_samples: false,
            union,
            rows: HashMap::new(),
            labels: HashMap::new(),
        }
    }

    /// The window to scan and keep for a request of `[start_ms, end_ms]`, or
    /// `None` when the result of the request is not to be kept.
    pub(crate) fn fill_window(&self, start_ms: i64, end_ms: i64) -> Option<(i64, i64)> {
        match self.union {
            None => Some((start_ms, end_ms)),
            Some((union_start_ms, union_end_ms))
                if start_ms >= union_start_ms && end_ms <= union_end_ms =>
            {
                Some((union_start_ms, union_end_ms))
            }
            Some(_) => None,
        }
    }
}
