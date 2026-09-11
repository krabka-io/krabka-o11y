use super::{Arc, JoinHandle, LogQueryAuthorizer};

/// The query authorizer a role checks tenants against, and the task that
/// connects it and keeps it fresh, when the role has one.
pub(crate) struct RoleQueryAuthorizer {
    pub(crate) authorizer: Arc<dyn LogQueryAuthorizer>,
    pub(crate) task: Option<(&'static str, JoinHandle<()>)>,
}
