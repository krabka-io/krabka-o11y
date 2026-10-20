use serde_json::{Value, json};

pub(crate) fn push_body() -> Value {
    json!({
        "streams": [{
            "stream": {"app": "api", "env": "prod"},
            "values": [
                ["10", "api error", {"trace_id": "abc"}],
                ["20", "api recovered"],
            ],
        }],
    })
}
