use super::{Response, json, success_data_response};

pub(crate) async fn targets() -> Response {
    success_data_response(json!({
        "activeTargets": [],
        "droppedTargets": [],
        "droppedTargetCounts": {},
    }))
}
