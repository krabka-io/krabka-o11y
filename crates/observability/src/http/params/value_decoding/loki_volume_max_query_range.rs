use super::*;

/// Widest window `/loki/api/v1/index/volume` and the range endpoints accept
/// (`Loki`'s 30d 1h default, to the nanosecond).
pub(crate) const LOKI_VOLUME_MAX_QUERY_RANGE: Time = secs(2_595_600);
