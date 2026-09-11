/// The group of Loki handlers whose answer to a tenant error a response copies.
///
/// Loki 3.5.1 does not answer a tenant error in one way. The auth middleware
/// answers a request without a tenant before any handler runs, so that answer
/// is the same on every surface. Each handler checks the tenant id itself, and
/// the error then takes the path that handler gives errors. Every variant
/// below is one such path, captured from the pinned Loki image with the
/// `loki_differential` configuration. Do not change a status to one that
/// looks more correct: a Grafana datasource shows the upstream answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TenantErrorSurface {
    /// `POST /loki/api/v1/push` and the delete-request API: 400 and the
    /// message with a line break.
    Push,
    /// `POST /otlp/v1/logs`: 400 and a `google.rpc.Status` body.
    OtlpPush,
    /// The label, series, index and detected-labels reads, and a metric
    /// query: 400 and the message without a line break. Two or more tenants
    /// get 500.
    Read,
    /// A range log query, the legacy instant query and the detected-fields
    /// reads, which reach Loki's querier over gRPC: 400 and the message after
    /// the gRPC status prefix. Two or more tenants get 500.
    QuerierRead,
    /// An instant log query: 500 and the message without a line break.
    InstantLogQuery,
    /// `GET /loki/api/v1/patterns`: 500 and the message. Two or more tenants
    /// get 404 and an empty body.
    Patterns,
    /// The tail websocket: 400 and the message, for every tenant error.
    Tail,
    /// The Loki ruler API: 500, and a JSON error body that says `no org id`
    /// under a `text/plain` content type.
    Ruler,
    /// `GET /prometheus/api/v1/rules` and `/prometheus/api/v1/alerts`: as
    /// [`TenantErrorSurface::Ruler`], with the text `no valid org id found`.
    PrometheusRuler,
}
