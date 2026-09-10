use super::{
    BTreeSet, COL_FINGERPRINT, COL_TIMESTAMP, Expr, MAX_FINGERPRINT_IN_LIST, SeriesFingerprint,
    col, lit,
};

/// Builds the scan predicate for one [`ScanTableRequest`](super::ScanTableRequest).
///
/// The predicate bounds the time column to `[min_ts, max_ts]` and the
/// fingerprint column to the matcher-resolved series. `DataFusion` hands both
/// to the Parquet source, which prunes row groups and data pages by their
/// statistics and, with filter pushdown on, decodes the payload columns only
/// for the rows that survive.
///
/// Returns `None` when neither half bounds anything, which leaves the scan
/// unfiltered rather than paying for a tautology.
pub(crate) fn scan_filter(
    min_ts: i64,
    max_ts: i64,
    fingerprints: &BTreeSet<SeriesFingerprint>,
) -> Option<Expr> {
    let time = (min_ts != i64::MIN || max_ts != i64::MAX)
        .then(|| col(COL_TIMESTAMP).between(lit(min_ts), lit(max_ts)));
    let series = fingerprint_filter(fingerprints);
    match (time, series) {
        (Some(time), Some(series)) => Some(time.and(series)),
        (Some(only), None) | (None, Some(only)) => Some(only),
        (None, None) => None,
    }
}

/// Bounds the fingerprint column to `fingerprints`.
///
/// Small sets become an exact `IN` list. A set too large for that (see
/// [`MAX_FINGERPRINT_IN_LIST`]) becomes the `[first, last]` range, which is a
/// superset: it still prunes blocks and row groups whose fingerprints all fall
/// outside the matched span, and it never drops a row the caller needs.
fn fingerprint_filter(fingerprints: &BTreeSet<SeriesFingerprint>) -> Option<Expr> {
    let first = *fingerprints.first()?;
    let last = *fingerprints.last()?;
    if fingerprints.len() > MAX_FINGERPRINT_IN_LIST {
        return Some(col(COL_FINGERPRINT).between(lit(first), lit(last)));
    }
    Some(col(COL_FINGERPRINT).in_list(fingerprints.iter().copied().map(lit).collect(), false))
}
