use super::*;

/// The admin client has no `MATCH` pattern filter, so one literal filter on
/// the topic name would miss a `*` ACL and every prefixed ACL. The three
/// filters are topic filters only, and never the whole cluster's ACL set.
#[test]
pub(crate) fn the_wal_topic_acl_filters_cover_the_topic_the_wildcard_and_every_prefix() {
    let filters = wal_topic_acl_filters("logs-wal");
    check!(filters.len() == 3);
    check!(filters[0].resource_type == Some(ResourceType::Topic));
    check!(filters[0].resource_name.as_deref() == Some("logs-wal"));
    check!(filters[0].pattern_type == Some(PatternType::Literal));
    check!(filters[1].resource_name.as_deref() == Some("*"));
    check!(filters[1].pattern_type == Some(PatternType::Literal));
    check!(filters[2].resource_name.is_none());
    check!(filters[2].pattern_type == Some(PatternType::Prefixed));
}
