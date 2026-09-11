/// A tenant asks to delete a range of its log lines: `POST` or `PUT` on
/// `/loki/api/v1/delete`.
pub const OPERATION_DELETE_REQUEST_CREATE: &str = "delete_request.create";

/// A tenant cancels a delete request: `DELETE` on `/loki/api/v1/delete`.
pub const OPERATION_DELETE_REQUEST_CANCEL: &str = "delete_request.cancel";

/// A tenant creates or replaces a rule group: `POST` on
/// `/loki/api/v1/rules/{namespace}` or on `/prometheus/config/v1/rules/{namespace}`.
///
/// The upstream API uses one request for both a new group and a replaced
/// group, so this operation covers both.
pub const OPERATION_RULE_GROUP_SET: &str = "rule_group.set";

/// A tenant deletes one rule group: `DELETE` on
/// `/loki/api/v1/rules/{namespace}/{group_name}` or on
/// `/prometheus/config/v1/rules/{namespace}/{group_name}`.
pub const OPERATION_RULE_GROUP_DELETE: &str = "rule_group.delete";

/// A tenant deletes every rule group in a namespace: `DELETE` on
/// `/loki/api/v1/rules/{namespace}` or on `/prometheus/config/v1/rules/{namespace}`.
pub const OPERATION_RULE_NAMESPACE_DELETE: &str = "rule_namespace.delete";

/// An operator changes the process log level: `POST` on `/log_level`.
pub const OPERATION_LOG_LEVEL_SET: &str = "log_level.set";

/// An operator flushes the ingester: `POST` on `/flush`.
pub const OPERATION_INGESTER_FLUSH: &str = "ingester.flush";

/// An operator shuts the ingester down: `GET` or `POST` on `/ingester/shutdown`.
pub const OPERATION_INGESTER_SHUTDOWN: &str = "ingester.shutdown";

/// An operator marks the ingester for shutdown: `POST` on
/// `/ingester/prepare_shutdown`.
pub const OPERATION_INGESTER_PREPARE_SHUTDOWN_SET: &str = "ingester.prepare_shutdown.set";

/// An operator removes the shutdown mark: `DELETE` on
/// `/ingester/prepare_shutdown`.
pub const OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET: &str = "ingester.prepare_shutdown.unset";

/// A principal reads a tenant's data: a query, a tail, or a rule read.
pub const OPERATION_TENANT_READ: &str = "tenant.read";

/// A principal writes a tenant's data: a push or an export.
pub const OPERATION_TENANT_WRITE: &str = "tenant.write";

/// A principal names a tenant before the service knows whether the request
/// reads or writes.
///
/// Use this operation for a principal-to-tenant refusal that occurs before
/// the route is known.
pub const OPERATION_TENANT_ACCESS: &str = "tenant.access";

/// A principal calls an admin operation, such as `POST /log_level`.
///
/// Use this operation for an admin refusal from
/// [`authorize_admin`](crate::server_security::authorize_admin), which happens
/// before the service knows which admin operation the request names.
pub const OPERATION_ADMIN_ACCESS: &str = "admin.access";

/// Every operation string that an audit event can carry.
pub const OPERATIONS: [&str; 14] = [
    OPERATION_DELETE_REQUEST_CREATE,
    OPERATION_DELETE_REQUEST_CANCEL,
    OPERATION_RULE_GROUP_SET,
    OPERATION_RULE_GROUP_DELETE,
    OPERATION_RULE_NAMESPACE_DELETE,
    OPERATION_LOG_LEVEL_SET,
    OPERATION_INGESTER_FLUSH,
    OPERATION_INGESTER_SHUTDOWN,
    OPERATION_INGESTER_PREPARE_SHUTDOWN_SET,
    OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET,
    OPERATION_TENANT_READ,
    OPERATION_TENANT_WRITE,
    OPERATION_TENANT_ACCESS,
    OPERATION_ADMIN_ACCESS,
];
