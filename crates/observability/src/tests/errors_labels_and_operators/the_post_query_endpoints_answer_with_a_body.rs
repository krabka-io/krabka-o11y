use super::*;

struct QueryResponseBody {
    status: axum::http::StatusCode,
    bytes: axum::body::Bytes,
}

struct PostQueryCase {
    name: &'static str,
    response: axum::response::Response,
}

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
        || axum::body::Bytes::from_static(b"query=%7Bapp%3D%22web%22%7D&start=0&end=10000000001");
    let request = || super::super::prelude::PostedQueryRequest {
        security: super::super::prelude::RequestSecurity::unauthenticated(),
        headers: headers.clone(),
        raw_query: None,
        body: body(),
    };
    let read = |response: axum::response::Response| async move {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("the response body is readable");
        QueryResponseBody { status, bytes }
    };

    let cases = [
        PostQueryCase {
            name: "detected_fields",
            response: super::super::prelude::detected_fields_post(State(state.clone()), request())
                .await
                .expect("detected-fields request succeeds"),
        },
        PostQueryCase {
            name: "detected_labels",
            response: super::super::prelude::detected_labels_post(State(state.clone()), request())
                .await
                .expect("detected-labels request succeeds"),
        },
        PostQueryCase {
            name: "index_volume",
            response: super::super::prelude::index_volume_post(State(state.clone()), request())
                .await
                .expect("index-volume request succeeds"),
        },
        PostQueryCase {
            name: "label_names",
            response: super::super::prelude::api_prom_label_names_post(
                State(state.clone()),
                request(),
            )
            .await
            .expect("label-names request succeeds"),
        },
    ];
    for case in cases {
        let answer = read(case.response.into_response()).await;
        check!(
            answer.status == axum::http::StatusCode::OK,
            "{}: {}",
            case.name,
            answer.status
        );
        let value: serde_json::Value =
            serde_json::from_slice(&answer.bytes).unwrap_or_else(|error| {
                panic!(
                    "{}: body is not JSON ({error}): {:?}",
                    case.name, answer.bytes
                )
            });
        if case.name == "index_volume" {
            check!(
                value["status"] == "success" && value["data"]["resultType"] == "vector",
                "{}: got {value}",
                case.name
            );
            check!(
                value["data"]["result"].as_array().map(Vec::len) == Some(0),
                "an empty store has no volume series: {value}"
            );
        } else {
            // An empty store answers with an empty JSON object, not with an
            // empty body -- a client parsing the response needs something
            // to parse.
            check!(value == serde_json::json!({}), "{}: got {value}", case.name);
        }
    }
}
