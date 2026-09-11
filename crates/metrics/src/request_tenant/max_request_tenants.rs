/// Most distinct tenants one metrics request can name.
///
/// Grafana Mimir reads a `|` in `X-Scope-OrgID` as a list of tenants for a
/// federated query. With tenant federation off, which is its default, it
/// accepts one tenant and reports this number in its rejection.
pub const MAX_REQUEST_TENANTS: usize = 1;
