use std::sync::Arc;

use axum::http::Method;

/// The routes that skip authentication when a credentials file is configured.
///
/// The default is `GET /ready` and `GET /metrics`. A Kubernetes probe and a
/// Prometheus scrape carry no credentials, and Mimir and Loki serve both
/// outside their authentication middleware. A match needs the same method and
/// the same path, so `POST /ready` and `GET /ready/` still need a credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnauthenticatedRoutes(Arc<[(Method, String)]>);

impl UnauthenticatedRoutes {
    /// A list with no routes, so every request needs a credential.
    #[must_use]
    pub fn none() -> Self {
        Self(Arc::from([]))
    }

    /// The list with one more route.
    #[must_use]
    pub fn with(self, method: Method, path: impl Into<String>) -> Self {
        let mut routes = self.0.to_vec();
        routes.push((method, path.into()));
        Self(Arc::from(routes))
    }

    /// Whether a request with `method` and `path` skips authentication.
    #[must_use]
    pub fn contains(&self, method: &Method, path: &str) -> bool {
        self.0
            .iter()
            .any(|(route_method, route_path)| route_method == method && route_path == path)
    }
}

impl Default for UnauthenticatedRoutes {
    fn default() -> Self {
        Self::none()
            .with(Method::GET, "/ready")
            .with(Method::GET, "/metrics")
    }
}
