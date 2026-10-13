use axum::{Json, response::IntoResponse as _};
use serde_json::json;

pub fn complete_trace_response() -> axum::response::Response {
    Json(json!({
        "trace": {
            "resourceSpans": [{
                "resource": { "attributes": [] },
                "scopeSpans": [{
                    "scope": {},
                    "spans": [{ "spanId": "BgYGBgYGBgY=", "name": "op" }]
                }]
            }]
        },
        "status": "COMPLETE",
        "message": ""
    }))
    .into_response()
}
