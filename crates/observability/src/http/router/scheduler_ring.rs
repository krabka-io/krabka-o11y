use super::{Response, ring_status_page};

pub(crate) async fn scheduler_ring() -> Response {
    // No readiness gate stands between this process and scheduling: the
    // querier role schedules its own work in-process, so the scheduler is
    // active from the moment the listener answers.
    ring_status_page("krabka-scheduler", "ACTIVE")
}
