use super::AclEntry;

/// What the broker answers when the logs path asks for the WAL topic's ACLs.
///
/// The two variants must not collapse into one list.
/// [`check_tenant_wal_read_acl`](super::check_tenant_wal_read_acl) and
/// [`check_tenant_wal_write_acl`](super::check_tenant_wal_write_acl) decide
/// each answer together with the principal of the request:
///
/// | ACLs | Unauthenticated request | Authenticated request |
/// | --- | --- | --- |
/// | `SecurityDisabled` | allow | allow |
/// | `Configured`, empty | allow | deny |
/// | `Configured`, not empty | the matching ACLs decide | the matching ACLs decide |
///
/// An empty list does not show that the broker enforces ACLs. The pinned
/// Krabka broker never answers Kafka error 54. Its default authorizer is
/// `AllowAllAuthorizer`, and that authorizer answers `DescribeAcls` with an
/// empty list. Error 54 is the answer of an Apache Kafka broker with no
/// authorizer. So a stock Krabka broker with no ACLs gives
/// `Configured(vec![])`.
///
/// Without a credentials file, the only ACL principal is `User:{tenant}`,
/// and the request names its tenant itself. A deny on an empty list then
/// refuses every tenant of a stock broker, and it adds no security. With a
/// credentials file, the principal is real, and no ACL grants it anything. So
/// the check denies, as the Kafka authorizer refuses a principal that no ACL
/// names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AclSet {
    /// The broker runs no authorizer. `DescribeAcls` answers Kafka error 54,
    /// `SECURITY_DISABLED`.
    SecurityDisabled,
    /// The broker holds these topic ACLs. An empty list allows only an
    /// unauthenticated request.
    Configured(Vec<AclEntry>),
}
