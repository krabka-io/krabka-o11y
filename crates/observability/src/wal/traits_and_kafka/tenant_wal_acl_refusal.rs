use super::{
    AclSet, PermissionType, Principal, TenantId, acl_matches_tenant_wal_read,
    acl_matches_tenant_wal_write,
};

/// The WAL topic operation a principal asks the ACLs for.
#[derive(Clone, Copy)]
pub(crate) enum WalTopicAccess {
    Read,
    Write,
}

impl WalTopicAccess {
    fn verb(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// One ACL decision: may `principal` perform `access` on `wal_topic` for
/// `tenant` under `acls`?
pub(crate) struct TenantWalAclCheck<'a> {
    pub(crate) principal: &'a Principal,
    pub(crate) tenant: &'a TenantId,
    pub(crate) wal_topic: &'a str,
    pub(crate) acls: &'a AclSet,
    pub(crate) access: WalTopicAccess,
}

/// Decides a WAL topic ACL check, returning the refusal reason when the ACLs
/// refuse it. [`check_tenant_wal_read_acl`](super::check_tenant_wal_read_acl)
/// documents the rules.
pub(crate) fn tenant_wal_acl_refusal(check: &TenantWalAclCheck<'_>) -> Option<String> {
    let TenantWalAclCheck {
        principal,
        tenant,
        wal_topic,
        acls,
        access,
    } = *check;
    let entries = match acls {
        AclSet::SecurityDisabled => return None,
        AclSet::Configured(entries)
            if entries.is_empty() && matches!(principal, Principal::Unauthenticated) =>
        {
            return None;
        }
        AclSet::Configured(entries) => entries,
    };
    let acl_principal = principal.acl_principal(tenant);
    let verb = access.verb();
    let mut allowed = false;
    for acl in entries {
        let matches = match access {
            WalTopicAccess::Read => acl_matches_tenant_wal_read(acl, &acl_principal, wal_topic),
            WalTopicAccess::Write => acl_matches_tenant_wal_write(acl, &acl_principal, wal_topic),
        };
        if !matches {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Some(format!(
                    "tenant {verb} ACL denied for WAL topic `{wal_topic}`"
                ));
            }
            PermissionType::Allow => allowed = true,
        }
    }

    (!allowed).then(|| format!("missing tenant {verb} ACL for WAL topic `{wal_topic}`"))
}
