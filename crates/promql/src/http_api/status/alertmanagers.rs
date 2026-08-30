use super::*;

pub(crate) async fn alertmanagers() -> Response {
    success_data_response(json!({
        "activeAlertmanagers": [],
        "droppedAlertmanagers": [],
    }))
}
