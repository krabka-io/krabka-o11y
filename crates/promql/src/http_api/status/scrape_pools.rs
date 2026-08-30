use super::{Response, json, success_data_response};

pub(crate) async fn scrape_pools() -> Response {
    success_data_response(json!([]))
}
