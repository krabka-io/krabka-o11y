use super::*;

/// `loki_vector_sample_value` reads the VALUE half of an instant sample --
/// index one, not zero -- and parses it. The timestamp beside it is also a
/// number, so reading the wrong index yields something that parses fine and
/// is simply wrong.
#[test]
pub(crate) fn a_loki_vector_sample_reads_its_value_and_not_its_timestamp() {
    let value =
        |sample: serde_json::Value| super::super::prelude::loki_vector_sample_value(&sample);
    let instant = |timestamp, sample_value| serde_json::json!({"metric": {}, "value": [timestamp, sample_value]});

    check!(value(instant(1_700_000_000_i64, "42")) == Some(MetricValue::new(42, 1)));
    check!(value(instant(1_700_000_000_i64, "1.5")) == Some(MetricValue::new(15, 10)));

    // The value is a STRING in Loki's encoding; a bare number is not read.
    check!(value(serde_json::json!({"value": [1, 42]})).is_none());
    // And an unparseable one is refused rather than defaulted to zero.
    check!(value(instant(1, "nonsense")).is_none());

    // Missing pieces: no value key, too short an array, not an array.
    check!(value(serde_json::json!({"metric": {}})).is_none());
    check!(value(serde_json::json!({"value": [1]})).is_none());
    check!(value(serde_json::json!({"value": "1"})).is_none());
}
