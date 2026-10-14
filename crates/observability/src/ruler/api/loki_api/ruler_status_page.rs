use super::{Response, status_html_page};

pub(crate) fn ruler_status_page() -> Response {
    status_html_page("Cortex Ruler Status", "")
}
