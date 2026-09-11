use super::{
    AllowAllQueryAuthorizer, Arc, CancellationToken, RoleQueryAuthorizer, RoleReadiness,
    ServiceConfig, ServiceDependencies, SwappableQueryAuthorizer, spawn_query_authorizer_connect,
};

/// The query authorizer a querier or a block builder checks tenants against,
/// and the task that connects it, when there is one.
///
/// An authorizer in `dependencies` is used as it is. Otherwise, with a broker
/// to connect to, the role gets a [`SwappableQueryAuthorizer`] that fails
/// closed until the broker-backed authorizer connects. The role must
/// supervise the returned task: it is also what keeps the ACL snapshot fresh.
/// `readiness` gets a `query-authorization` gate that the connect marks.
///
/// With no broker at all, which is an embedded router or a test, every tenant
/// is allowed, as [`crate::QuerierState::new`] allows every tenant.
pub(crate) fn query_authorizer_for_role(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
    token: &CancellationToken,
    readiness: &RoleReadiness,
) -> RoleQueryAuthorizer {
    if let Some(authorizer) = &dependencies.query_authorizer {
        return RoleQueryAuthorizer {
            authorizer: Arc::clone(authorizer),
            task: None,
        };
    }
    let Some(connect) = dependencies.deferred_query_authorizer_connect.clone() else {
        return RoleQueryAuthorizer {
            authorizer: Arc::new(AllowAllQueryAuthorizer),
            task: None,
        };
    };
    let (swappable, slot) = SwappableQueryAuthorizer::new();
    let task = spawn_query_authorizer_connect(
        connect,
        slot,
        config.querier_dependency_reconnect_interval,
        token.clone(),
        readiness.gate("query-authorization"),
    );
    RoleQueryAuthorizer {
        authorizer: Arc::new(swappable),
        task: Some(("query authorization", task)),
    }
}
