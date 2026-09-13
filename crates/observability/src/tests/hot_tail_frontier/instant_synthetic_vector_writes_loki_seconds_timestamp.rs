use super::*;

/// A synthetic vector sample carries its timestamp in seconds, as Loki writes
/// every instant sample. Loki 3.5.1 answers `vector(1)` at
/// `time=4000000000000000000` with `[4000000000, "1"]`.
#[test]
pub(crate) fn instant_synthetic_vector_writes_loki_seconds_timestamp() {
    let response = loki_instant_scalar_or_vector_response(
        4_000_000_000_000_000_000,
        ScalarVectorExpressionResult::Vector {
            sample: Some("1".to_string()),
            metric: BTreeMap::new(),
        },
    );

    check!(response["data"]["result"][0]["value"][0] == json!(4_000_000_000_i64));
}
