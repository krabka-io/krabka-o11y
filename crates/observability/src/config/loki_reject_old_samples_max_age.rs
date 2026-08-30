use super::*;

/// `Loki`'s `reject_old_samples_max_age` default: samples older than this are
/// refused on ingest.
pub(crate) const LOKI_REJECT_OLD_SAMPLES_MAX_AGE: Time = days(7);
