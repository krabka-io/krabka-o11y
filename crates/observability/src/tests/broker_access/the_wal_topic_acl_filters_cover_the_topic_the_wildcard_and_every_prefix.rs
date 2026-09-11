use super::*;

/// The admin client has no `MATCH` pattern filter, so one literal filter on
/// the topic name would miss a `*` ACL and every prefixed ACL. The three
/// filters are topic filters only, and never the whole cluster's ACL set.
#[test]
pub(crate) fn the_wal_topic_acl_filters_cover_the_topic_the_wildcard_and_every_prefix() {
    let topic = |pattern_type, resource_name: Option<&str>| AclEntryFilter {
        resource_type: Some(ResourceType::Topic),
        resource_name: resource_name.map(str::to_owned),
        pattern_type: Some(pattern_type),
        ..AclEntryFilter::default()
    };
    check!(
        wal_topic_acl_filters("logs-wal")
            == [
                topic(PatternType::Literal, Some("logs-wal")),
                topic(PatternType::Literal, Some("*")),
                topic(PatternType::Prefixed, None),
            ]
    );
}
