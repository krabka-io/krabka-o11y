use super::*;

#[test]
pub(crate) fn acl_helpers_require_topic_operation_principal_and_pattern() {
    let allow_write = acl_entry(
        ResourceType::Topic,
        "__krabka_observability_logs_wal",
        PatternType::Literal,
        "User:tenant-a",
        AclOperation::Write,
        PermissionType::Allow,
    );
    let allow_read = acl_entry(
        ResourceType::Topic,
        "__krabka_",
        PatternType::Prefixed,
        "User:*",
        AclOperation::Read,
        PermissionType::Allow,
    );
    let deny_write = acl_entry(
        ResourceType::Topic,
        "*",
        PatternType::Literal,
        "User:tenant-a",
        AclOperation::All,
        PermissionType::Deny,
    );

    for (entry, topic, want) in [
        (&allow_write, "__krabka_observability_logs_wal", true),
        (&allow_read, "__krabka_observability_logs_wal", true),
        (&allow_read, "other-topic", false),
    ] {
        check!(
            matches_acl_topic_pattern(entry, topic) == want,
            "pattern={} topic={topic}",
            entry.resource_name
        );
    }
    // A literal "*" resource name matches any topic. Neither entry in the
    // loop above asks it: one names the topic, the other is a prefix.
    check!(matches_acl_topic_pattern(
        &deny_write,
        "some-unrelated-topic"
    ));

    check!(acl_matches_tenant_wal_write(
        &allow_write,
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));

    // The wildcard principal grants on the write side too. Only the read
    // entry above carries one, so the write side's own check was free.
    check!(acl_matches_tenant_wal_write(
        &acl_entry(
            ResourceType::Topic,
            "__krabka_observability_logs_wal",
            PatternType::Literal,
            "User:*",
            AclOperation::Write,
            PermissionType::Allow,
        ),
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));

    // And a non-Topic resource is refused reading, as it already is
    // writing.
    check!(!acl_matches_tenant_wal_read(
        &acl_entry(
            ResourceType::Group,
            "__krabka_observability_logs_wal",
            PatternType::Literal,
            "User:tenant-a",
            AclOperation::Read,
            PermissionType::Allow,
        ),
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));
    check!(acl_matches_tenant_wal_read(
        &allow_read,
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));

    // A concrete principal grants itself and nobody else. `allow_read`
    // above carries the wildcard, so its second arm answered for both and
    // nothing yet separated "this principal" from "any principal but this
    // one".
    let read_as = |principal: &str| {
        acl_entry(
            ResourceType::Topic,
            "__krabka_observability_logs_wal",
            PatternType::Literal,
            principal,
            AclOperation::Read,
            PermissionType::Allow,
        )
    };
    check!(acl_matches_tenant_wal_read(
        &read_as("User:tenant-a"),
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));
    check!(!acl_matches_tenant_wal_read(
        &read_as("User:tenant-b"),
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));
    check!(!acl_matches_tenant_wal_write(
        &allow_read,
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));
    check!(!acl_matches_tenant_wal_read(
        &allow_write,
        "User:tenant-a",
        "__krabka_observability_logs_wal"
    ));
    check!(!acl_matches_tenant_wal_write(
        &acl_entry(
            ResourceType::Group,
            "__krabka_observability_logs_wal",
            PatternType::Literal,
            "User:tenant-a",
            AclOperation::Write,
            PermissionType::Allow,
        ),
        "User:tenant-a",
        "__krabka_observability_logs_wal",
    ));
    check!(
        check_tenant_wal_write_acl(
            "tenant-a",
            "__krabka_observability_logs_wal",
            std::slice::from_ref(&allow_write)
        )
        .is_ok()
    );
    check!(
        check_tenant_wal_read_acl(
            "tenant-a",
            "__krabka_observability_logs_wal",
            std::slice::from_ref(&allow_read)
        )
        .is_ok()
    );
    check!(
        check_tenant_wal_write_acl("tenant-a", "__krabka_observability_logs_wal", &[deny_write])
            .is_err()
    );
    check!(
        check_tenant_wal_read_acl(
            "tenant-a",
            "__krabka_observability_logs_wal",
            &[allow_write]
        )
        .is_err()
    );
}
