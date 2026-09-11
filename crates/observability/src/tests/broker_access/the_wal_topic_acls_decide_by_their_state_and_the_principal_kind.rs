use super::{broker_access_for_test::WAL_TOPIC, *};

fn grafana_for_test() -> Principal {
    Principal::Authenticated {
        name: Arc::from("grafana"),
        method: AuthMethod::Bearer,
        tenants: TenantGrant::All,
        admin: false,
        events: SecurityEventSink::default(),
    }
}

fn grant_for_test(principal: &str, operation: AclOperation) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "__krabka_".to_string(),
        pattern_type: PatternType::Prefixed,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type: PermissionType::Allow,
    }
}

// The pinned Krabka broker answers `DescribeAcls` with an empty list when it
// holds no ACLs, so an empty list does not show that the broker enforces
// ACLs. It allows a request whose only ACL principal is its self-named
// tenant, and refuses an authenticated principal that no ACL grants. A
// non-empty list decides both kinds by the ACL principal that
// `Principal::acl_principal` names: `User:tenant-a` for the unauthenticated
// request, and `User:grafana` for the authenticated one.
#[test]
pub(crate) fn the_wal_topic_acls_decide_by_their_state_and_the_principal_kind() {
    let tenant = tenant_for_test("tenant-a");
    let unauthenticated = Principal::Unauthenticated;
    let grafana = grafana_for_test();
    let for_tenant =
        || AclSet::Configured(vec![grant_for_test("User:tenant-a", AclOperation::All)]);
    let for_grafana =
        || AclSet::Configured(vec![grant_for_test("User:grafana", AclOperation::All)]);
    let cases = [
        (
            "security disabled",
            AclSet::SecurityDisabled,
            &unauthenticated,
            (true, true),
        ),
        (
            "security disabled",
            AclSet::SecurityDisabled,
            &grafana,
            (true, true),
        ),
        (
            "empty",
            AclSet::Configured(Vec::new()),
            &unauthenticated,
            (true, true),
        ),
        (
            "empty",
            AclSet::Configured(Vec::new()),
            &grafana,
            (false, false),
        ),
        (
            "granted to the tenant",
            for_tenant(),
            &unauthenticated,
            (true, true),
        ),
        (
            "granted to the tenant",
            for_tenant(),
            &grafana,
            (false, false),
        ),
        (
            "granted to grafana",
            for_grafana(),
            &unauthenticated,
            (false, false),
        ),
        ("granted to grafana", for_grafana(), &grafana, (true, true)),
        (
            "a read grant to grafana",
            AclSet::Configured(vec![grant_for_test("User:grafana", AclOperation::Read)]),
            &grafana,
            (true, false),
        ),
        (
            "a write grant to grafana",
            AclSet::Configured(vec![grant_for_test("User:grafana", AclOperation::Write)]),
            &grafana,
            (false, true),
        ),
    ];

    for (name, acls, principal, expected) in cases {
        let decided = (
            check_tenant_wal_read_acl(principal, &tenant, WAL_TOPIC, &acls).is_ok(),
            check_tenant_wal_write_acl(principal, &tenant, WAL_TOPIC, &acls).is_ok(),
        );
        check!(decided == expected, "{name}, {principal:?}");
    }

    let empty = AclSet::Configured(Vec::new());
    check!(matches!(
        check_tenant_wal_read_acl(&grafana, &tenant, WAL_TOPIC, &empty),
        Err(QueryAuthorizationError::Unauthorized { tenant, reason })
            if tenant == "tenant-a"
                && reason == format!("missing tenant read ACL for WAL topic `{WAL_TOPIC}`")
    ));
    check!(matches!(
        check_tenant_wal_write_acl(&grafana, &tenant, WAL_TOPIC, &empty),
        Err(IngestLimitError::Unauthorized { tenant, reason })
            if tenant == "tenant-a"
                && reason == format!("missing tenant write ACL for WAL topic `{WAL_TOPIC}`")
    ));
}
