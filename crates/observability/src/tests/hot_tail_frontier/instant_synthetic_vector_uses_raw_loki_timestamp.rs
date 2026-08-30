use super::*;

#[test]
pub(crate) fn instant_synthetic_vector_uses_raw_loki_timestamp() {
    let response = loki_instant_scalar_or_vector_response(
        4_000_000_000,
        ScalarVectorExpressionResult::Vector {
            sample: Some("1".to_string()),
            metric: BTreeMap::new(),
        },
    );

    assert_eq!(
        response["data"]["result"][0]["value"][0],
        json!(4_000_000_000i64)
    );
}
