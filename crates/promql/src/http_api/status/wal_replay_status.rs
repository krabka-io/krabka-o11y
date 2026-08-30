use super::*;

pub(crate) async fn wal_replay_status() -> Response {
    success_data_response(json!({
        "min": 0,
        "max": 0,
        "current": 0,
        "state": "done",
    }))
}
