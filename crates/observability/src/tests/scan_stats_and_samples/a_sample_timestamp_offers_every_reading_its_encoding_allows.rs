use super::*;

/// `metric_binary_sample_timestamp_ns_candidates` offers every reading a
/// sample's timestamp could plausibly have. Which readings depends on how
/// it was encoded, and each JSON type takes its own branch: an integer is
/// ambiguous and offers two, a float is seconds and offers one, a string
/// may parse either way and offers whichever succeed.
#[test]
pub(crate) fn a_sample_timestamp_offers_every_reading_its_encoding_allows() {
    let candidates = |timestamp: serde_json::Value| {
        super::super::prelude::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!([
            timestamp, "1"
        ]))
    };

    // An integer is ambiguous: both the raw value and it read as seconds.
    check!(candidates(serde_json::json!(5)) == Some(vec![5, 5_000_000_000]));
    // Zero collapses to one reading, since both are the same number.
    check!(candidates(serde_json::json!(0)) == Some(vec![0]));

    // A float is seconds, rounded to the nearest nanosecond, and offers
    // only that -- there is no second reading to be ambiguous about.
    check!(candidates(serde_json::json!(5.5)) == Some(vec![5_500_000_000]));
    // Rounded, not truncated. 5.5 lands on a whole nanosecond and cannot
    // show the difference; 1.7 nanoseconds rounds up to 2 where flooring
    // gives 1, which is the sub-nanosecond precision a float carries and
    // an integer count cannot.
    check!(candidates(serde_json::json!(1.7e-9)) == Some(vec![2]));

    // A string is tried both ways and offers whichever parse. "5" has no
    // decimal point so only the integer reading applies; "5.5" is the
    // reverse.
    check!(candidates(serde_json::json!("5")) == Some(vec![5, 5_000_000_000]));
    check!(candidates(serde_json::json!("5.5")) == Some(vec![5_500_000_000]));

    // Nothing parses, or there is nothing to parse.
    check!(candidates(serde_json::json!("nonsense")).is_none());
    check!(candidates(serde_json::json!(true)).is_none());
    check!(
        super::prelude::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!([]))
            .is_none()
    );
    check!(
        super::prelude::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!("bare"))
            .is_none()
    );
}
