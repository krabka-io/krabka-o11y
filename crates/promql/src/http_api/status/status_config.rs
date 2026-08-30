use super::*;

pub(crate) async fn status_config() -> Response {
    success_data_response(json!({
        "yaml": "global:\n  scrape_interval: 1m\n",
    }))
}
