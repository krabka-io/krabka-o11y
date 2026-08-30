use super::*;

#[test]
pub(crate) fn instant_scalar_expression_keeps_loki_seconds_timestamp() {
    let response = loki_instant_scalar_or_vector_response(
        4_000_000_000,
        ScalarVectorExpressionResult::Scalar {
            sample: "2".to_string(),
        },
    );

    assert_eq!(response["data"]["result"][0], json!(4));
}
