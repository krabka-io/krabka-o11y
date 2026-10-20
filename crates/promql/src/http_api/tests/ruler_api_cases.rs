use super::*;

mod expand_alert_template_substitutions {
    use super::*;

    #[test]
    pub(crate) fn expand_alert_template_substitutions() {
        let mut labels = Labels::new();
        labels.insert("job", "api");
        labels.insert("instance", "host-1");

        assert2::assert!(
            expand_alert_template("value is {{ $value }}", 42.5, &labels) == "value is 42.5"
        );
        assert2::assert!(expand_alert_template("job={{ $labels.job }}", 1.0, &labels) == "job=api");
        assert2::assert!(
            expand_alert_template("job={{ index $labels \"job\" }}", 1.0, &labels) == "job=api"
        );
        // Absent label expands to empty string.
        assert2::assert!(expand_alert_template("x={{ $labels.missing }}", 1.0, &labels) == "x=");
        assert2::assert!(
            expand_alert_template(
                "{{ if $labels.job }}{{ humanize $value }} {{ printf \"%s\" $labels.job | title }}{{ end }}",
                1500.0,
                &labels,
            ) == "1.5k Api"
        );
        // No-whitespace variants still expand.
        assert2::assert!(
            expand_alert_template("{{$value}} {{$labels.job}}", 7.0, &labels) == "7 api"
        );

        let mut external_labels = Labels::new();
        external_labels.insert("cluster", "prod-eu");
        assert2::assert!(
            expand_alert_template_with_external(
                "{{ $externalLabels.cluster }} {{ $externalURL }}",
                1.0,
                &labels,
                &external_labels,
                "https://prom.example",
            ) == "prod-eu https://prom.example"
        );
    }
}

mod ruler_config_mutations_are_audited {
    use std::net::SocketAddr;

    use assert2::check;
    use axum::extract::ConnectInfo;
    use krabka_audit::{AuditLog, AuditStats};
    use krabka_observability::{
        audit::{
            AuditClocks, AuditEvent, AuditOutcome, EpochMs, MECHANISM_BEARER,
            OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET, OPERATION_RULE_NAMESPACE_DELETE,
            OPERATION_TENANT_ACCESS, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE, RESOURCE_TENANT,
            admin_operation, principal, resource, source_endpoint, unknown_source_endpoint,
        },
        server_security::PeerAddr,
    };

    use super::*;

    const OPS_TOKEN: &str = "ops-token-5f1c9d2e7a4b8c3d6e0f1a2b3c4d5e6f";
    const OPS_TOKEN_SHA256: &str =
        "fd66d679ee99eef3efa2ef4127680aea063ca35c6b394cacad365fdeec1203be";
    const GROUP_YAML: &str =
        "name: latency\nrules:\n  - record: job:up:sum\n    expr: sum by (job) (up)\n";

    fn client() -> SocketAddr {
        "10.0.0.7:51000".parse().expect("a valid socket address")
    }

    async fn send(
        app: &Router,
        method: &str,
        uri: &str,
        tenant: &str,
        body: &'static str,
    ) -> StatusCode {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("Authorization", format!("Bearer {OPS_TOKEN}"))
            .header("X-Scope-OrgID", tenant)
            .header("Content-Type", "application/yaml")
            .body(Body::from(body))
            .expect("a valid request");
        request.extensions_mut().insert(ConnectInfo(PeerAddr {
            socket: client(),
            client_identity: None,
        }));
        app.clone()
            .oneshot(request)
            .await
            .expect("the router answers")
            .status()
    }

    /// The event with its time set to zero, so a test can compare whole events.
    fn at_time_zero(event: AuditEvent) -> AuditEvent {
        match event {
            AuditEvent::AdminOperation {
                outcome,
                principal,
                source,
                operation,
                resources,
                ..
            } => AuditEvent::AdminOperation {
                outcome,
                principal,
                source,
                operation,
                resources,
                time_ms: 0,
            },
            AuditEvent::AuthorizationDenied {
                principal,
                source,
                resource_type,
                resource_name,
                operation,
                ..
            } => AuditEvent::AuthorizationDenied {
                principal,
                source,
                resource_type,
                resource_name,
                operation,
                time_ms: 0,
            },
            other => other,
        }
    }

    /// Each ruler config mutation records one admin operation with its principal,
    /// its client address, the tenant, namespace and group it changed, and whether
    /// it succeeded. A read records nothing. A principal refused the tenant is
    /// recorded once, by the server's security events, and not again as an
    /// operation.
    #[tokio::test]
    pub(crate) async fn ruler_config_mutations_are_audited() {
        let directory = tempfile::tempdir().expect("a temporary directory");
        let credentials = directory.path().join("credentials.yaml");
        std::fs::write(
        &credentials,
        format!(
            "principals:\n  - name: ops\n    token_sha256: [\"{OPS_TOKEN_SHA256}\"]\n    tenants: [\"tenant-a\"]\n"
        ),
    )
    .expect("the credentials file is writable");
        let audit_log = AuditLog::new(64);
        let log = audit_log.0;
        let mut events = audit_log.1;
        let audit = AuditHandle::new(
            log,
            Arc::new(AuditStats::new()),
            AuditClocks::system().clock,
        );
        let security = super::server_security_args(credentials)
            .load()
            .expect("the credentials load")
            .with_security_events(Arc::new(audit.clone()));
        let state = Arc::new(
            PrometheusApiState::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default())
                .with_audit(audit),
        );
        let app = authenticate_requests(super::super::super::prometheus_router(state), &security);

        let statuses = [
            send(
                &app,
                "POST",
                "/prometheus/config/v1/rules/team",
                "tenant-a",
                GROUP_YAML,
            )
            .await,
            send(
                &app,
                "POST",
                "/prometheus/config/v1/rules/team",
                "tenant-a",
                "name: [",
            )
            .await,
            send(
                &app,
                "GET",
                "/prometheus/config/v1/rules/team",
                "tenant-a",
                "",
            )
            .await,
            send(
                &app,
                "DELETE",
                "/prometheus/config/v1/rules/team/latency",
                "tenant-a",
                "",
            )
            .await,
            send(
                &app,
                "DELETE",
                "/prometheus/config/v1/rules/team",
                "tenant-a",
                "",
            )
            .await,
            send(
                &app,
                "DELETE",
                "/prometheus/config/v1/rules/team",
                "tenant-b",
                "",
            )
            .await,
        ];
        let mut recorded = Vec::new();
        while let Ok(event) = events.try_recv() {
            recorded.push(at_time_zero(event));
        }

        check!(
            statuses
                == [
                    StatusCode::ACCEPTED,
                    StatusCode::BAD_REQUEST,
                    StatusCode::OK,
                    StatusCode::ACCEPTED,
                    StatusCode::NOT_FOUND,
                    StatusCode::FORBIDDEN,
                ]
        );
        let ops = principal("ops", MECHANISM_BEARER);
        let tenant_a = resource(RESOURCE_TENANT, "tenant-a");
        let namespace = resource(RESOURCE_RULE_NAMESPACE, "team");
        let group = resource(RESOURCE_RULE_GROUP, "team/latency");
        check!(
            recorded
                == vec![
                    admin_operation(
                        ops.clone(),
                        source_endpoint(client()),
                        OPERATION_RULE_GROUP_SET,
                        vec![tenant_a.clone(), namespace.clone(), group.clone()],
                        AuditOutcome::Success,
                        EpochMs(0),
                    ),
                    admin_operation(
                        ops.clone(),
                        source_endpoint(client()),
                        OPERATION_RULE_GROUP_SET,
                        vec![tenant_a.clone(), namespace.clone()],
                        AuditOutcome::Failure,
                        EpochMs(0),
                    ),
                    admin_operation(
                        ops.clone(),
                        source_endpoint(client()),
                        OPERATION_RULE_GROUP_DELETE,
                        vec![tenant_a.clone(), namespace.clone(), group],
                        AuditOutcome::Success,
                        EpochMs(0),
                    ),
                    admin_operation(
                        ops.clone(),
                        source_endpoint(client()),
                        OPERATION_RULE_NAMESPACE_DELETE,
                        vec![tenant_a, namespace],
                        AuditOutcome::Failure,
                        EpochMs(0),
                    ),
                    AuditEvent::AuthorizationDenied {
                        principal: ops,
                        source: unknown_source_endpoint(),
                        resource_type: RESOURCE_TENANT.to_owned(),
                        resource_name: "tenant-b".to_owned(),
                        operation: OPERATION_TENANT_ACCESS.to_owned(),
                        time_ms: 0,
                    },
                ]
        );
    }
}

mod ruler_tenants_lists_each_configured_tenant {
    use super::*;

    #[test]
    fn ruler_tenants_lists_each_configured_tenant() {
        let state = PrometheusApiState::new(
            Arc::new(InMemoryMetricStore::new()),
            crate::EngineOpts::default(),
        );
        let mut rules = state.ruler_rules.write().expect("ruler rules lock");
        rules.insert(TenantId::new("tenant-b").unwrap(), BTreeMap::new());
        rules.insert(TenantId::new("tenant-a").unwrap(), BTreeMap::new());
        drop(rules);

        assert2::assert!(
            state
                .ruler_tenants()
                .iter()
                .map(TenantId::as_str)
                .collect::<Vec<_>>()
                == vec!["tenant-a", "tenant-b"]
        );
    }
}
