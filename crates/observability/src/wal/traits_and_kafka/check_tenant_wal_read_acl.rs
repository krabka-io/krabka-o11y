use super::{
    AclSet, Principal, QueryAuthorizationError, TenantId, TenantWalAclCheck, WalTopicAccess,
    tenant_wal_acl_refusal,
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
    let refusal = tenant_wal_acl_refusal(&TenantWalAclCheck {
        principal,
        tenant,
        wal_topic,
        acls,
        access: WalTopicAccess::Read,
    });
    refusal.map_or(Ok(()), |reason| {
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason,
        })
    })
}
