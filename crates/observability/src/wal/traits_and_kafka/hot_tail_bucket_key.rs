use super::{Time, TimeExt};

/// Maps a record timestamp to its bucket key.
///
/// `bucket_width` is the width of a hot-tail time bucket. It is coarse enough
/// that a wide retention window holds few buckets, and fine enough that a
/// typical query window of minutes to hours only touches the buckets it
/// overlaps. The function uses [`i64::div_euclid`], so negative, pre-epoch,
/// timestamps still bucket monotonically, and the bucket that contains a given
/// timestamp is unambiguous.
pub(crate) fn hot_tail_bucket_key(timestamp_ns: i64, bucket_width: Time) -> i64 {
    timestamp_ns.div_euclid(bucket_width.nanos_i64())
}
