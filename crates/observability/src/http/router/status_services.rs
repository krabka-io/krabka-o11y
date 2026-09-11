use super::{
    DRAINING_GATE, LOKI_SERVICE_MODULES, Response, RoleReadiness, StatusCode, text_response,
};

/// The `/services` page, reporting each module's state.
///
/// `Loki` lists the modules its process runs and the state each is in. Krabka
/// serves the whole `Loki` surface from every role, so the list is the
/// single-binary one -- see [`LOKI_CONFIG_TARGET`](super::LOKI_CONFIG_TARGET)
/// -- but the states are this process's own. A module reads `Starting` while
/// any readiness gate is unmet, which is exactly when `/ready` answers 503, so
/// the two pages cannot disagree. Before this took its state from
/// [`RoleReadiness`] it said `Running` for everything from the first bound
/// port onwards, and a rollout watching `/services` saw a healthy process at
/// the same moment `/ready` was refusing traffic.
///
/// `server` is the exception: the HTTP listener answered the request that got
/// here, so it is running whatever else is still coming up.
pub(crate) fn status_services(readiness: &RoleReadiness) -> Response {
    let pending = readiness.pending();
    let state = if pending.is_empty() {
        "Running"
    } else if pending.contains(&DRAINING_GATE) {
        // An operator asked this process to leave rotation. `Loki` calls that
        // `Stopping`, and calling it `Starting` would send a runbook the wrong
        // way at the one moment it is being read.
        "Stopping"
    } else {
        "Starting"
    };
    let mut page = String::new();
    for module in LOKI_SERVICE_MODULES {
        let module_state = if *module == "server" {
            "Running"
        } else {
            state
        };
        page.push_str(module);
        page.push_str(" => ");
        page.push_str(module_state);
        page.push('\n');
    }
    text_response(StatusCode::OK, &page)
}
