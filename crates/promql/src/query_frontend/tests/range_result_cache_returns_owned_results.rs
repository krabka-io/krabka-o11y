use super::*;

#[tokio::test]
pub(crate) async fn range_result_cache_returns_owned_results() {
    let cache = QueryFrontendCache::default();
    let query = up_range_query(0, None);
    let result = one_sample_matrix(labels(&[("__name__", "up")]));

    cache.insert("tenant-a", &query, result).await.unwrap();
    let Some(AnnotatedQueryResult {
        result: QueryResult::RangeMatrix(mut first_hit),
        ..
    }) = cache.get("tenant-a", &query).await.unwrap()
    else {
        panic!("cached range matrix");
    };
    first_hit[0].samples.clear();

    let Some(AnnotatedQueryResult {
        result: QueryResult::RangeMatrix(second_hit),
        ..
    }) = cache.get("tenant-a", &query).await.unwrap()
    else {
        panic!("cached range matrix");
    };
    assert2::assert!(second_hit[0].samples == vec![(0, SampleValue::Float(1.0))]);
}
