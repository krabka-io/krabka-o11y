use super::*;

pub(crate) async fn scheduler_ring() -> Response {
    ring_status_page("krabka-scheduler")
}
