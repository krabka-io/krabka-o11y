use super::*;

/// `histogram_stdvar` weights each bucket by the square of the distance from
/// its representative value to the mean, and that value is the geometric
/// midpoint -- negated for a bucket below zero, arithmetic for one spanning
/// it. The `sum` here is deliberately not zero: at a mean of zero the squaring
/// hides the sign, so a negative bucket's midpoint could come back positive
/// and the variance would not move.
#[tokio::test]
pub(crate) async fn histogram_stdvar_places_each_bucket_at_its_own_midpoint() {
    let engine = signed_bucket_histogram_engine(12.0);

    for (query, want) in [
        ("histogram_stdvar(h)", 3.459_559_885_480_119_5),
        ("histogram_stddev(h)", 1.859_989_216_495_654_6),
    ] {
        assert_lone_value(&engine, query, want).await;
    }
}
