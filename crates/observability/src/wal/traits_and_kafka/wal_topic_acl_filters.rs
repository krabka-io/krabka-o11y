use super::{AclEntryFilter, PatternType, ResourceType};

/// The `DescribeAcls` filters whose union holds every ACL that can apply to
/// `wal_topic`.
///
/// `krabka-client-admin` exposes only the `LITERAL` and `PREFIXED` pattern
/// filters, not Kafka's `MATCH`. A literal filter on the topic name returns
/// only the ACLs that name the topic exactly, and misses a literal `*` and
/// every prefixed ACL. So three filters are necessary: the topic by name, the
/// wildcard by name, and every prefixed topic ACL. Each one is narrower than
/// the whole cluster's ACL set.
pub(crate) fn wal_topic_acl_filters(wal_topic: &str) -> [AclEntryFilter; 3] {
    let topic = |pattern_type, resource_name: Option<&str>| AclEntryFilter {
        resource_type: Some(ResourceType::Topic),
        resource_name: resource_name.map(str::to_owned),
        pattern_type: Some(pattern_type),
        ..AclEntryFilter::default()
    };
    [
        topic(PatternType::Literal, Some(wal_topic)),
        topic(PatternType::Literal, Some("*")),
        topic(PatternType::Prefixed, None),
    ]
}
