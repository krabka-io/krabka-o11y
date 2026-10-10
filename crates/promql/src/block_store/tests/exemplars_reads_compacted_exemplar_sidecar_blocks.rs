use super::*;

#[tokio::test]
pub(crate) async fn exemplars_reads_compacted_exemplar_sidecar_blocks() {
    let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
    let batch = exemplar_batch(
        series_labels.fingerprint(),
        10_500,
        7.0,
        "abc",
        "def",
        "kind",
        "slow",
    );
    let store = manifest_store(ManifestBlock {
        kind: MetricBlockKind::Exemplars,
        block_key: "metrics/exemplars/0003.parquet",
        schema: exemplar_schema(),
        batch,
        last_offset: 0,
        series_labels: series_labels.clone(),
    })
    .await;
    let exemplars = api_exemplars(&store).await;

    check!(exemplars.len() == 1);
    check!(exemplars[0].series_labels == series_labels);
    check!(exemplars[0].labels.get("trace_id") == Some("abc"));
    check!(exemplars[0].labels.get("span_id") == Some("def"));
    check!(exemplars[0].labels.get("kind") == Some("slow"));
    check!(exemplars[0].ts_ms == 10_500);
    check!((exemplars[0].value - 7.0).abs() < f64::EPSILON);
}
