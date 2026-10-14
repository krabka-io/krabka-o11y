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
    struct AclDecisionCase<'a> {
        name: &'static str,
        acls: AclSet,
        principal: &'a Principal,
        read_allowed: bool,
        write_allowed: bool,
    }
    let cases = [
        AclDecisionCase {
            name: "security disabled",
            acls: AclSet::SecurityDisabled,
            principal: &unauthenticated,
            read_allowed: true,
            write_allowed: true,
        },
        AclDecisionCase {
            name: "security disabled",
            acls: AclSet::SecurityDisabled,
            principal: &grafana,
            read_allowed: true,
            write_allowed: true,
        },
        AclDecisionCase {
            name: "empty",
            acls: AclSet::Configured(Vec::new()),
            principal: &unauthenticated,
            read_allowed: true,
            write_allowed: true,
        },
        AclDecisionCase {
            name: "empty",
            acls: AclSet::Configured(Vec::new()),
            principal: &grafana,
            read_allowed: false,
            write_allowed: false,
        },
        AclDecisionCase {
            name: "granted to the tenant",
            acls: for_tenant(),
            principal: &unauthenticated,
            read_allowed: true,
            write_allowed: true,
        },
        AclDecisionCase {
            name: "granted to the tenant",
            acls: for_tenant(),
            principal: &grafana,
            read_allowed: false,
            write_allowed: false,
        },
        AclDecisionCase {
            name: "granted to grafana",
            acls: for_grafana(),
            principal: &unauthenticated,
            read_allowed: false,
            write_allowed: false,
        },
        AclDecisionCase {
            name: "granted to grafana",
            acls: for_grafana(),
            principal: &grafana,
            read_allowed: true,
            write_allowed: true,
        },
        AclDecisionCase {
            name: "a read grant to grafana",
            acls: AclSet::Configured(vec![grant_for_test("User:grafana", AclOperation::Read)]),
            principal: &grafana,
            read_allowed: true,
            write_allowed: false,
        },
        AclDecisionCase {
            name: "a write grant to grafana",
            acls: AclSet::Configured(vec![grant_for_test("User:grafana", AclOperation::Write)]),
            principal: &grafana,
            read_allowed: false,
            write_allowed: true,
        },
    ];

    for case in cases {
        check!(
            check_tenant_wal_read_acl(case.principal, &tenant, WAL_TOPIC, &case.acls).is_ok()
                == case.read_allowed,
            "{} read, {:?}",
            case.name,
            case.principal
        );
        check!(
            check_tenant_wal_write_acl(case.principal, &tenant, WAL_TOPIC, &case.acls).is_ok()
                == case.write_allowed,
            "{} write, {:?}",
            case.name,
            case.principal
        );
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
