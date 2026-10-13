use tokio_util::sync::DropGuard;

use super::{
    RuntimeSecurity, ServiceConfig, ServiceDependencies, ServiceRuntimeError,
    start_runtime_security,
};

pub(crate) struct AuditedRuntimeSecurity {
    pub(crate) security: RuntimeSecurity,
    pub(crate) stop_guard: DropGuard,
    pub(crate) dependencies: ServiceDependencies,
}

pub(crate) async fn start_audited_runtime_security(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
) -> Result<AuditedRuntimeSecurity, ServiceRuntimeError> {
    let security = start_runtime_security(config, &dependencies).await?;
    let stop_guard = security.audit_stop.clone().drop_guard();
    let dependencies = dependencies.with_audit(security.audit.clone());
    Ok(AuditedRuntimeSecurity { security, stop_guard, dependencies })
}
