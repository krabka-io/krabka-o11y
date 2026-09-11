use super::{Time, minutes};

/// How often an accumulator looks for stale delta streams to drop.
///
/// The pass walks every stream the tenant has, so it must not run per request.
/// It runs at most once per this interval, over a map the stream cap already
/// bounds. A stream therefore survives `otlp_delta_max_stale` by up to one
/// interval.
pub(crate) const DELTA_PRUNE_INTERVAL: Time = minutes(1);
