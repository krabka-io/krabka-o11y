use super::*;

/// Two samples share an instant if any of their candidate readings agree.
/// A bare integer is ambiguous -- Prometheus writes timestamps in seconds
/// and Loki in nanoseconds -- so each yields both readings, and 5 matches
/// `5_000_000_000` because they are the same moment spelled differently.
/// That is the whole reason the comparison is over LISTS rather than
/// values, and a fixture using one spelling throughout never shows it.
#[test]
pub(crate) fn two_samples_share_an_instant_if_any_reading_of_them_agrees() {
    let matches =
        |left, right| super::super::prelude::metric_binary_sample_timestamps_match(&left, &right);
    let at = |timestamp: serde_json::Value| serde_json::json!([timestamp, "1"]);

    // The same number, and the same instant written two ways.
    check!(matches(at(serde_json::json!(5)), at(serde_json::json!(5))));
    check!(
        matches(
            at(serde_json::json!(5)),
            at(serde_json::json!(5_000_000_000_i64))
        ),
        "seconds and nanoseconds for the same moment"
    );
    check!(
        matches(
            at(serde_json::json!(5_000_000_000_i64)),
            at(serde_json::json!(5))
        ),
        "and the other way round"
    );

    // Different instants, in either spelling.
    check!(!matches(at(serde_json::json!(5)), at(serde_json::json!(7))));
    check!(!matches(
        at(serde_json::json!(5)),
        at(serde_json::json!(7_000_000_000_i64))
    ));

    // Neither side parses: they fall back to comparing the raw values, so
    // two identical unparseable timestamps still pair up and two different
    // ones do not.
    check!(matches(
        at(serde_json::json!("nonsense")),
        at(serde_json::json!("nonsense"))
    ));
    check!(!matches(
        at(serde_json::json!("nonsense")),
        at(serde_json::json!("other"))
    ));

    // One side parses and the other does not: no match, rather than
    // falling through to a raw comparison that would never agree anyway.
    check!(!matches(
        at(serde_json::json!(5)),
        at(serde_json::json!("nonsense"))
    ));
    check!(!matches(
        at(serde_json::json!("nonsense")),
        at(serde_json::json!(5))
    ));
}
