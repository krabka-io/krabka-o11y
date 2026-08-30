use super::*;

pub(crate) fn acl_matches_tenant_wal_read(
    acl: &AclEntry,
    principal: &str,
    wal_topic: &str,
) -> bool {
    acl.resource_type == ResourceType::Topic
        && matches!(acl.operation, AclOperation::All | AclOperation::Read)
        && (acl.principal == principal || acl.principal == "User:*")
        && matches_acl_topic_pattern(acl, wal_topic)
}
