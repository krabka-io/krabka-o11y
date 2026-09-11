/// A tenant. The resource name is the tenant ID.
pub const RESOURCE_TENANT: &str = "tenant";

/// A delete request. The resource name is the request ID.
pub const RESOURCE_DELETE_REQUEST: &str = "delete_request";

/// A rule group. The resource name is `{namespace}/{group_name}`.
pub const RESOURCE_RULE_GROUP: &str = "rule_group";

/// A rule namespace. The resource name is the namespace.
pub const RESOURCE_RULE_NAMESPACE: &str = "rule_namespace";

/// The process log level. The resource name is the new level.
pub const RESOURCE_LOG_LEVEL: &str = "log_level";

/// An ingester. The resource name is the instance ID.
pub const RESOURCE_INGESTER: &str = "ingester";

/// A write-ahead log topic. The resource name is the topic name.
pub const RESOURCE_WAL_TOPIC: &str = "wal_topic";

/// The service's admin operations as a whole: the ops routes such as
/// `/log_level`, `/flush` and `/ingester/shutdown`.
pub const RESOURCE_ADMIN_API: &str = "admin_api";

/// Every resource-type string that an audit event can carry.
pub const RESOURCE_TYPES: [&str; 8] = [
    RESOURCE_TENANT,
    RESOURCE_DELETE_REQUEST,
    RESOURCE_RULE_GROUP,
    RESOURCE_RULE_NAMESPACE,
    RESOURCE_LOG_LEVEL,
    RESOURCE_INGESTER,
    RESOURCE_WAL_TOPIC,
    RESOURCE_ADMIN_API,
];
