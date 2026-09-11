use super::{
    Arc, AuditService, CancellationToken, RuntimeSecurity, ServiceConfig, ServiceDependencies,
    ServiceRuntimeError, krabka_product,
};

/// Loads the data-port security of a role and starts its audit layer.
///
/// A security or an audit handle in `dependencies` wins over the flags in
/// `config`. Otherwise this loads `config.server_security`, and it starts the
/// audit layer that `config.audit` names. That layer writes to the WAL
/// bootstrap servers unless `--audit-bootstrap` names others, under the WAL
/// client security of `dependencies`. With no flag set, the data port serves
/// plain HTTP, and the audit layer is disabled and spawns no task.
///
/// Every security event of the listener goes to the audit handle.
///
/// # Errors
///
/// Returns [`ServiceRuntimeError::ServerSecurity`] when the data-port security
/// flags do not load, and [`ServiceRuntimeError::Audit`] when the audit layer
/// does not start.
pub(crate) async fn start_runtime_security(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
) -> Result<RuntimeSecurity, ServiceRuntimeError> {
    let server = match &dependencies.server_security {
        Some(security) => security.clone(),
        None => config.server_security.load()?,
    };
    let audit_stop = CancellationToken::new();
    let (audit, audit_writer) = match &dependencies.audit {
        Some(audit) => (audit.clone(), None),
        None => AuditService::start(
            &config.audit,
            krabka_product(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
            config.wal_bootstrap_server.as_deref(),
            dependencies.wal_security.as_ref(),
            audit_stop.clone(),
        )
        .await?
        .into_parts(),
    };
    Ok(RuntimeSecurity {
        server: server.with_security_events(Arc::new(audit.clone())),
        audit,
        audit_writer,
        audit_stop,
    })
}
