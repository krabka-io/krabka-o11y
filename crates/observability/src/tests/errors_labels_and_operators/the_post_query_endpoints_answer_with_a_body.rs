use super::*;

/// The four POST query endpoints each answer with a JSON body. Replacing
/// any of them with a default `Response` yields an empty 200 -- a status
/// check alone accepts that, so the body has to be read.
#[tokio::test]
pub(crate) async fn the_post_query_endpoints_answer_with_a_body() {
    use axum::{extract::State, response::IntoResponse as _};

    let dir = tempfile::TempDir::new().expect("temp dir");
    let state = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default());
    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
    let body =
        || axum::body::Bytes::from_static(b"query=%7Bapp%3D%22web%22%7D&start=0&end=1000000000");
    let read = |response: axum::response::Response| async move {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("the response body is readable");
        (status, bytes)
    };

    for (name, response) in [
        (
            "detected_fields",
            super::super::prelude::detected_fields_post(
                State(state.clone()),
                headers.clone(),
                axum::extract::RawQuery(None),
                body(),
            )
            .await,
        ),
        (
            "detected_labels",
            super::super::prelude::detected_labels_post(
                State(state.clone()),
                headers.clone(),
                axum::extract::RawQuery(None),
                body(),
            )
            .await,
        ),
        (
            "index_volume",
            super::super::prelude::index_volume_post(
                State(state.clone()),
                headers.clone(),
                axum::extract::RawQuery(None),
                body(),
            )
            .await,
        ),
        (
            "label_names",
            super::super::prelude::api_prom_label_names_post(
                State(state.clone()),
                headers.clone(),
                axum::extract::RawQuery(None),
                body(),
            )
            .await,
        ),
    ] {
        let (status, bytes) = read(response.into_response()).await;
        check!(status == axum::http::StatusCode::OK, "{name}: {status}");
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .unwrap_or_else(|error| panic!("{name}: body is not JSON ({error}): {bytes:?}"));
        if name == "index_volume" {
            check!(
                value["status"] == "success" && value["data"]["resultType"] == "vector",
                "{name}: got {value}"
            );
            check!(
                value["data"]["result"].as_array().map(Vec::len) == Some(0),
                "an empty store has no volume series: {value}"
            );
        } else {
            // An empty store answers with an empty JSON object, not with an
            // empty body -- a client parsing the response needs something
            // to parse.
            check!(value == serde_json::json!({}), "{name}: got {value}");
        }
    }
}
