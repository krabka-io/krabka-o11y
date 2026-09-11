use super::*;

/// `loki_boltdb_shipper_compactor_running` is the one line in the status
/// page that varies by component. It reads 1 on a process that runs `Loki`'s
/// compactor, which here is the block builder and the all-in-one that
/// contains it, and 0 on the roles that do not write durable storage.
#[tokio::test]
pub(crate) async fn the_status_page_flags_only_the_compactor_as_running() {
    for (component, running) in [
        ("compactor", 1),
        ("all", 1),
        ("querier", 0),
        ("distributor", 0),
    ] {
        let response = status_metrics(component);
        check!(response.status() == StatusCode::OK, "{component}");
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("the response body is readable");
        let body = String::from_utf8(bytes.to_vec()).expect("utf-8");
        check!(
            body.contains(&format!(
                "loki_boltdb_shipper_compactor_running {running}\n"
            )),
            "{component}: {body}"
        );
        check!(
            body.contains(&format!(
                "krabka_observability_service_up{{component=\"{component}\"}} 1"
            )),
            "{component}"
        );
    }
}
