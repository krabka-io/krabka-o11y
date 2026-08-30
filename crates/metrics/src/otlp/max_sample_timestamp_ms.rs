/// Sane upper bound for an ingested sample timestamp, in milliseconds. This
/// module rejects a data point beyond this bound and does not translate it into
/// an absurd far-future millisecond value, which would poison the per-series
/// out-of-order and too-old window. `7_258_118_400_000` is
/// `2200-01-01T00:00:00Z`. That is well past any legitimate metric timestamp and
/// still reachable from a `u64` `time_unix_nano`, whose ceiling is about the
/// year 2554. An absurd point such as `u64::MAX` is therefore rejected, which
/// matches how Prometheus rejects a future sample.
pub(crate) const MAX_SAMPLE_TIMESTAMP_MS: u64 = 7_258_118_400_000;
