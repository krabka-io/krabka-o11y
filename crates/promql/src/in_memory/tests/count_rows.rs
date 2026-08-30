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
