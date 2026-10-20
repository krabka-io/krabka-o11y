use krabka_observability::audit::AuditBuildError;

use super::{
    AuditHandle, AuditService, CancellationToken, Cli, ClientSecurity, SupervisedTasks,
    krabka_product,
};

/// The audit layer of one process: the handle request handlers record events
/// through, and the supervised task that writes them.
pub struct RoleAudit {
    pub handle: AuditHandle,
    pub tasks: SupervisedTasks,
}

/// Starts the audit layer that `cli.audit` configures.
///
/// With no `--audit-topic` this spawns nothing and reaches no broker.
pub async fn start_role_audit(
    cli: &Cli,
    wal_security: Option<&ClientSecurity>,
) -> Result<RoleAudit, AuditBuildError> {
    let audit_stop = CancellationToken::new();
    let parts = AuditService::start(
        &cli.audit,
        krabka_product("krabka-metrics-service", env!("CARGO_PKG_VERSION")),
        cli.wal_bootstrap.as_deref(),
        wal_security,
        audit_stop.clone(),
    )
    .await?
    .into_parts();
    let mut tasks = SupervisedTasks::new(audit_stop);
    if let Some(writer) = parts.writer {
        tasks.adopt("audit writer", writer);
    }
    Ok(RoleAudit {
        handle: parts.handle,
        tasks,
    })
}
