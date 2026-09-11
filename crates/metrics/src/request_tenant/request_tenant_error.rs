use super::{
    Error, IntoResponse, MAX_REQUEST_TENANTS, Response, StatusCode, TenantResolveError,
    X_CONTENT_TYPE_OPTIONS,
};

/// Why a metrics request does not resolve to one tenant.
///
/// Each message is the text the pinned Grafana Mimir 2.16.1 image sends. A
/// Grafana datasource shows that text to its user, so Krabka uses the same
/// words.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum RequestTenantError {
    /// The request names no tenant, or a tenant that is not a valid tenant id.
    #[error(transparent)]
    Resolve(#[from] TenantResolveError),

    /// The request names more than [`MAX_REQUEST_TENANTS`] distinct tenants.
    #[error(
        "too many tenant IDs present in the request. max: {MAX_REQUEST_TENANTS} actual: {actual}"
    )]
    TooManyTenants {
        /// The number of distinct tenants the request names.
        actual: usize,
    },
}

impl RequestTenantError {
    /// The HTTP status Mimir answers this error with.
    ///
    /// A missing or invalid tenant is `401 Unauthorized`. Too many tenants is
    /// `422 Unprocessable Entity`.
    #[must_use]
    pub const fn http_status(&self) -> StatusCode {
        match self {
            Self::Resolve(_) => StatusCode::UNAUTHORIZED,
            Self::TooManyTenants { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        }
    }
}

impl IntoResponse for RequestTenantError {
    // Mimir rejects the tenant before any API handler runs, through Go's
    // `http.Error`. That writes plain text with a trailing line break, so the
    // body is never the JSON error envelope of the Prometheus API.
    fn into_response(self) -> Response {
        (
            self.http_status(),
            [(X_CONTENT_TYPE_OPTIONS, "nosniff")],
            format!("{self}\n"),
        )
            .into_response()
    }
}
