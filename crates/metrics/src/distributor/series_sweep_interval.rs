use super::{Time, minutes};

/// How often the series tracker looks for idle series to evict.
///
/// The sweep walks every tracked tenant, so it must not run per request. It
/// runs at most once per this interval, over a map the tenant cap already
/// bounds. A series therefore survives its tenant's idle timeout by up to one
/// interval. The value is the period at which Mimir's ingester refreshes its
/// own active-series set, `-ingester.active-series-metrics-update-period`.
pub(crate) const SERIES_SWEEP_INTERVAL: Time = minutes(1);
