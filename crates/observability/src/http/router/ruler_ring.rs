use super::{Response, ruler_status_page};

pub(crate) async fn ruler_ring() -> Response {
    ruler_status_page()
}
