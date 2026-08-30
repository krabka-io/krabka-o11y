use super::*;

#[test]
pub(crate) fn range_query_merge_rejects_non_matrix_subquery_results() {
    let err = merge_range_query_results(vec![QueryResult::Scalar {
        ts_ms: 0,
        value: 1.0,
    }])
    .unwrap_err();

    assert2::assert!(format!("{err}").contains("range matrix"));
}
