use super::{json, Response, success_data_response};

pub(crate) async fn alertmanagers() -> Response {
    success_data_response(json!({
        "activeAlertmanagers": [],
        "droppedAlertmanagers": [],
    }))
}
