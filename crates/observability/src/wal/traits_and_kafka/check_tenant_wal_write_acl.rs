use super::{
    AclSet, IngestLimitError, Principal, TenantId, TenantWalAclCheck, WalTopicAccess,
    tenant_wal_acl_refusal,
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
    let refusal = tenant_wal_acl_refusal(&TenantWalAclCheck {
        principal,
        tenant,
        wal_topic,
        acls,
        access: WalTopicAccess::Write,
    });
    refusal.map_or(Ok(()), |reason| {
        Err(IngestLimitError::Unauthorized {
            tenant: tenant.to_string(),
            reason,
        })
    })
}
