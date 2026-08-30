use super::*;

pub(crate) fn check_tenant_wal_write_acl(
    tenant: &str,
    wal_topic: &str,
    acls: &[AclEntry],
) -> Result<(), IngestLimitError> {
    if acls.is_empty() {
        return Ok(());
    }

    let principal = format!("User:{tenant}");
    let mut allowed = false;
    for acl in acls {
        if !acl_matches_tenant_wal_write(acl, &principal, wal_topic) {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Err(IngestLimitError::Unauthorized {
                    tenant: tenant.to_string(),
                    reason: format!("tenant write ACL denied for WAL topic `{wal_topic}`"),
                });
            }
            PermissionType::Allow => allowed = true,
        }
    }

    if allowed {
        Ok(())
    } else {
        Err(IngestLimitError::Unauthorized {
            tenant: tenant.to_string(),
            reason: format!("missing tenant write ACL for WAL topic `{wal_topic}`"),
        })
    }
}
