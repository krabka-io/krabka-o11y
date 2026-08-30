use super::*;

pub(crate) fn acl_entry(
    resource_type: ResourceType,
    resource_name: &str,
    pattern_type: PatternType,
    principal: &str,
    operation: AclOperation,
    permission_type: PermissionType,
) -> AclEntry {
    AclEntry {
        resource_type,
        resource_name: resource_name.to_string(),
        pattern_type,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type,
    }
}
