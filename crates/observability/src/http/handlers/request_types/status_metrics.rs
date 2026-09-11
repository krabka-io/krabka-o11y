use super::{IntoResponse, Response, StatusCode};

pub(crate) fn status_metrics(component: &'static str) -> Response {
    // `Loki` sets this to 1 on any process that runs its compactor, which for
    // Krabka means the role that writes durable storage -- the block builder
    // -- and the all-in-one that contains it. A `Loki` dashboard keyed on this
    // gauge would otherwise report a stack with no compactor running at all.
    let compactor_running = usize::from(matches!(component, "compactor" | "all"));
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        format!(
            "# HELP loki_build_info A metric with a constant '1' value labeled by version, revision, branch, goversion from which loki was built, and the goos and goarch for the build.\n\
             # TYPE loki_build_info gauge\n\
             loki_build_info{{branch=\"unknown\",goarch=\"unknown\",goos=\"unknown\",goversion=\"unknown\",revision=\"unknown\",tags=\"\",version=\"{}\"}} 1\n\
             # HELP loki_boltdb_shipper_compactor_running Value will be 1 if compactor is currently running on this instance\n\
             # TYPE loki_boltdb_shipper_compactor_running gauge\n\
             loki_boltdb_shipper_compactor_running {compactor_running}\n\
             # HELP krabka_observability_service_up Whether the observability service is running.\n\
             # TYPE krabka_observability_service_up gauge\n\
             krabka_observability_service_up{{component=\"{component}\"}} 1\n",
            env!("CARGO_PKG_VERSION")
        ),
    )
        .into_response()
}
