use super::{
    AclSet, IngestLimitError, PermissionType, Principal, TenantId, acl_matches_tenant_wal_write,
};

/// Allows `principal` to write the data of `tenant` to the WAL topic when the
/// ACLs allow it.
///
/// The ACL principal is [`Principal::acl_principal`]: `User:{name}` for an
/// authenticated request, and `User:{tenant}` for an unauthenticated request.
///
/// - [`AclSet::SecurityDisabled`] allows every write.
/// - An empty [`AclSet::Configured`] allows an unauthenticated write, and it
///   refuses an authenticated write. [`AclSet`] gives the reason, with the
///   evidence from the pinned Krabka broker.
/// - A non-empty [`AclSet::Configured`] allows the write only when an `Allow`
///   ACL matches the ACL principal and no `Deny` ACL matches it.
///
/// # Errors
///
/// Returns [`IngestLimitError::Unauthorized`] when the ACLs refuse the write.
pub(crate) fn check_tenant_wal_write_acl(
    principal: &Principal,
    tenant: &TenantId,
    wal_topic: &str,
    acls: &AclSet,
) -> Result<(), IngestLimitError> {
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
        if !acl_matches_tenant_wal_write(acl, &acl_principal, wal_topic) {
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
