use super::{
    AclSet, PermissionType, Principal, QueryAuthorizationError, TenantId,
    acl_matches_tenant_wal_read,
};

/// Allows `principal` to read the data of `tenant` from the WAL topic when the
/// ACLs allow it.
///
/// The ACL principal is [`Principal::acl_principal`]: `User:{name}` for an
/// authenticated request, and `User:{tenant}` for an unauthenticated request.
///
/// - [`AclSet::SecurityDisabled`] allows every read.
/// - An empty [`AclSet::Configured`] allows an unauthenticated read, and it
///   refuses an authenticated read. [`AclSet`] gives the reason, with the
///   evidence from the pinned Krabka broker.
/// - A non-empty [`AclSet::Configured`] allows the read only when an `Allow`
///   ACL matches the ACL principal and no `Deny` ACL matches it.
///
/// # Errors
///
/// Returns [`QueryAuthorizationError::Unauthorized`] when the ACLs refuse the
/// read.
pub(crate) fn check_tenant_wal_read_acl(
    principal: &Principal,
    tenant: &TenantId,
    wal_topic: &str,
    acls: &AclSet,
) -> Result<(), QueryAuthorizationError> {
    let entries = match acls {
        AclSet::SecurityDisabled => return Ok(()),
        AclSet::Configured(entries)
            if entries.is_empty() && matches!(principal, Principal::Unauthenticated) =>
        {
            return Ok(());
        }
        AclSet::Configured(entries) => entries,
    };
    let acl_principal = principal.acl_principal(tenant);
    let mut allowed = false;
    for acl in entries {
        if !acl_matches_tenant_wal_read(acl, &acl_principal, wal_topic) {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Err(QueryAuthorizationError::Unauthorized {
                    tenant: tenant.to_string(),
                    reason: format!("tenant read ACL denied for WAL topic `{wal_topic}`"),
                });
            }
            PermissionType::Allow => allowed = true,
        }
    }

    if allowed {
        Ok(())
    } else {
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason: format!("missing tenant read ACL for WAL topic `{wal_topic}`"),
        })
    }
}
