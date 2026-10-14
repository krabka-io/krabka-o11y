use super::*;

#[tokio::test]
pub(crate) async fn discovery_rejects_label_counts_over_tenant_series_limit() {
    let uris = [
        "/api/v1/labels?start=0&end=1",
        "/api/v1/label/job/values?start=0&end=1",
    ];

    for uri in uris {
        assert_series_limit_rejected(uri).await;
    }
}
