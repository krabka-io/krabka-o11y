use super::*;

pub(crate) fn matches_acl_topic_pattern(acl: &AclEntry, wal_topic: &str) -> bool {
    match acl.pattern_type {
        PatternType::Literal => acl.resource_name == wal_topic || acl.resource_name == "*",
        PatternType::Prefixed => wal_topic.starts_with(&acl.resource_name),
    }
}
