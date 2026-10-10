use super::*;

pub(crate) async fn count_rows(result: &ScanResult, table: &str) -> i64 {
    let df = result
        .ctx
        .sql(&format!("SELECT count(*) AS c FROM {table}"))
        .await
        .unwrap();
    let output = df.collect().await.unwrap();
    output[0].column(0).as_primitive::<Int64Type>().value(0)
}

/// Counts the float rows of tenant `t`'s `up` series over all time.
pub(crate) async fn count_up_float_rows(store: &InMemoryMetricStore) -> i64 {
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let scan = store
        .scan("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    let table = scan.float_table.clone().unwrap();
    count_rows(&scan, &table).await
}
