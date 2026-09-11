use super::{Arc, AuditHandle};

/// The audit handle of a service, with the names that its audit events use.
///
/// The service runtime puts one into the extensions of every request. A
/// router that runs in process, without the runtime, has none, and its
/// requests use [`ServiceAudit::disabled`].
#[derive(Clone, Debug)]
pub(crate) struct ServiceAudit {
    /// The handle that records the events.
    pub(crate) handle: AuditHandle,
    /// The WAL topic that a broker ACL refusal names.
    pub(crate) wal_topic: Arc<str>,
    /// The instance that an ingester operation names.
    ///
    /// This is the instance that the role's `/ring` page shows.
    pub(crate) instance: Arc<str>,
    /// Whether the service reads a credentials file.
    ///
    /// When it does, a request that carries no principal and came through no
    /// listener is refused. Such a request reached a router that the service
    /// serves some other way than through `serve_router`, which would skip the
    /// credentials check.
    pub(crate) authentication_required: bool,
}

impl ServiceAudit {
    /// An audit trail that records nothing.
    pub(crate) fn disabled() -> Self {
        Self {
            handle: AuditHandle::disabled(),
            wal_topic: Arc::from(""),
            instance: Arc::from(""),
            authentication_required: false,
        }
    }
}
