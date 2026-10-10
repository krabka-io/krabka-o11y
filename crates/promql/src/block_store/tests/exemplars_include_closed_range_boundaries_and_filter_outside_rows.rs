use super::*;

#[tokio::test]
pub(crate) async fn exemplars_include_closed_range_boundaries_and_filter_outside_rows() {
    let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = exemplar_batch_from_rows(&[
        (fp, 9_999, 1.0, "too-low", "s1", "kind", "outside"),
        (fp, 10_000, 2.0, "start", "s2", "kind", "inside"),
        (fp, 11_000, 3.0, "end", "s3", "kind", "inside"),
        (fp, 11_001, 4.0, "too-high", "s4", "kind", "outside"),
    ]);
    let store = manifest_store(ManifestBlock {
        kind: MetricBlockKind::Exemplars,
        block_key: "metrics/exemplars/0005.parquet",
        schema: exemplar_schema(),
        batch,
        last_offset: 3,
        series_labels: series_labels.clone(),
    })
    .await;
    let exemplars = api_exemplars(&store).await;

    check!(exemplars.len() == 2);
    for (row, trace_id, span_id, ts_ms, value) in [
        (0_usize, "start", "s2", 10_000_i64, 2.0_f64),
        (1, "end", "s3", 11_000, 3.0),
    ] {
        check!(exemplars[row].series_labels == series_labels, "row {row}");
        check!(
            exemplars[row].labels.get("trace_id") == Some(trace_id),
            "row {row}"
        );
        check!(
            exemplars[row].labels.get("span_id") == Some(span_id),
            "row {row}"
        );
        check!(
            exemplars[row].labels.get("kind") == Some("inside"),
            "row {row}"
        );
        check!(exemplars[row].ts_ms == ts_ms, "row {row}");
        check!(
            exemplars[row].value.to_bits() == value.to_bits(),
            "row {row}"
        );
    }
}
