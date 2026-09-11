use super::{
    ALL_OPS, Arc, AuditHandle, BLOCK_BUILDER_OPS, DISTRIBUTOR_OPS, QUERIER_OPS, Role,
    ServerSecurity, ServiceAudit, ServiceConfig, ServiceDependencies,
};

/// The audit context that the handlers of `config.target` record through.
///
/// The handle is the one in `dependencies`, or a disabled handle. A broker ACL
/// refusal names `config.wal_topic`. An ingester operation names the instance
/// that the role's `/ring` page shows. Authentication is required when the
/// injected server security, or else the configured flags, name a credentials
/// file. That is the same test `ServerSecurityArgs::load` applies.
pub(crate) fn service_audit_for_config(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
) -> ServiceAudit {
    let ops = match config.target {
        Role::Distributor => DISTRIBUTOR_OPS,
        Role::Querier => QUERIER_OPS,
        Role::BlockBuilder => BLOCK_BUILDER_OPS,
        Role::All => ALL_OPS,
    };
    ServiceAudit {
        handle: dependencies
            .audit
            .clone()
            .unwrap_or_else(AuditHandle::disabled),
        wal_topic: Arc::from(config.wal_topic.as_str()),
        instance: Arc::from(ops.ring_component),
        authentication_required: dependencies.server_security.as_ref().map_or_else(
            || config.server_security.auth_credentials_config.is_some(),
            ServerSecurity::authentication_enabled,
        ),
    }
}
