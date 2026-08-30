use super::*;

#[test]
pub(crate) fn hot_tail_bucket_key_uses_euclidean_minutes() {
    let bucket_width = minutes(1);
    assert_eq!(hot_tail_bucket_key(0, bucket_width), 0);
    check!(hot_tail_bucket_key(bucket_width.nanos_i64() - 1, bucket_width) == 0);
    check!(hot_tail_bucket_key(bucket_width.nanos_i64(), bucket_width) == 1);
    assert_eq!(hot_tail_bucket_key(-1, bucket_width), -1);
    check!(hot_tail_bucket_key(-bucket_width.nanos_i64(), bucket_width) == -1);
    check!(hot_tail_bucket_key(-bucket_width.nanos_i64() - 1, bucket_width) == -2);
    check!(hot_tail_bucket_key(minutes(2).nanos_i64(), minutes(2)) == 1);
}
