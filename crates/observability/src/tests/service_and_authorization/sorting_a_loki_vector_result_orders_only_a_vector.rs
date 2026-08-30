use super::*;

/// Sorting a Loki vector result orders it by sample value, and touches
/// nothing else: a matrix carries the same shape but must come back in the
/// order it arrived. Nothing had called this at all, so returning without
/// doing anything -- or sorting exactly the results it should not -- both
/// passed.
#[test]
pub(crate) fn sorting_a_loki_vector_result_orders_only_a_vector() {
    let sample = |value: &str| serde_json::json!({"metric": {"n": value}, "value": [0, value]});
    let order = |value: &serde_json::Value| {
        value
            .pointer("/data/result")
            .and_then(serde_json::Value::as_array)
            .expect("a result array")
            .iter()
            .map(|entry| {
                entry
                    .pointer("/metric/n")
                    .and_then(serde_json::Value::as_str)
                    .expect("a name")
                    .to_string()
            })
            .collect::<Vec<_>>()
    };

    let mut vector = serde_json::json!({
        "data": { "resultType": "vector", "result": [sample("3"), sample("1"), sample("2")] }
    });
    super::super::prelude::sort_loki_vector_result(&mut vector, false);
    check!(order(&vector) == vec!["1", "2", "3"], "ascending");

    super::super::prelude::sort_loki_vector_result(&mut vector, true);
    check!(
        order(&vector) == vec!["3", "2", "1"],
        "descending reverses it"
    );

    // Same shape, different result type: left exactly as it came.
    let mut matrix = serde_json::json!({
        "data": { "resultType": "matrix", "result": [sample("3"), sample("1")] }
    });
    super::super::prelude::sort_loki_vector_result(&mut matrix, false);
    check!(
        order(&matrix) == vec!["3", "1"],
        "a matrix is not reordered"
    );
}
