use super::{Router, catch_panic};

/// Wraps `router` so a panic in any of its handlers becomes a 500 for that
/// request instead of a dropped connection.
///
/// Apply it once per listener, at the point the router is served, rather than
/// inside each router builder. A role composes several routers into one app --
/// a data router merged with `/ready`, a Prometheus router built in another
/// crate -- and one layer at the serving boundary covers all of them without
/// stacking one wrapper per merge.
pub fn contain_handler_panics(router: Router) -> Router {
    router.layer(axum::middleware::from_fn(catch_panic))
}
