use super::*;

mod an_unconfigured_state_enforces_the_default_query_limits {
    use super::*;

    struct EngineCaps {
        max_samples: usize,
        max_fetched_series: usize,
    }

    struct ProcessCapsCase {
        name: &'static str,
        process: EngineOpts,
        tenant_limits: Option<Limits>,
        expected: EngineCaps,
    }

    /// Mimir applies its default limits without a runtime config, so a querier
    /// started with no overrides file must still cap what one query selects. A
    /// state built without `with_query_limits` enforces `Limits::default()` at the
    /// HTTP gate and in the engine.
    ///
    /// The engine options are process caps, as Mimir's `-querier.max-samples` is:
    /// a tenant's cap lowers a process cap and never raises it, and a tenant cap of
    /// zero leaves the process cap in force. Without that rule a default tenant
    /// limit would silently override `--query-max-samples`.
    #[test]
    pub(crate) fn an_unconfigured_state_enforces_the_default_query_limits() {
        let tenant = TenantId::new("tenant-a").expect("a valid tenant id");
        let default_series = usize::try_from(Limits::default().max_fetched_series_per_query)
            .expect("the default cap fits a usize");
        let engine_caps = |process: EngineOpts, limits: Option<Limits>| {
            let state = PrometheusApiState::new(Arc::new(two_series_store()), process);
            let state = match limits {
                Some(limits) => state.with_query_limits(OverridesProvider::new(limits)),
                None => state,
            };
            let opts = state.engine_for_tenant(&tenant).opts;
            EngineCaps {
                max_samples: opts.max_samples,
                max_fetched_series: opts.max_fetched_series,
            }
        };

        let unconfigured =
            PrometheusApiState::new(Arc::new(two_series_store()), EngineOpts::default());
        assert2::assert!(
            enforce_selected_series_limit(&unconfigured, &tenant, default_series).is_ok()
        );
        assert2::assert!(
            enforce_selected_series_limit(&unconfigured, &tenant, default_series + 1).is_err()
        );

        let process = EngineOpts {
            max_samples: 100,
            max_fetched_series: 7,
            ..EngineOpts::default()
        };
        let tenant_caps = |samples, series| Limits {
            max_samples_per_query: samples,
            max_fetched_series_per_query: series,
            ..Limits::default()
        };
        let cases = [
            ProcessCapsCase {
                name: "default limits, no process series cap",
                process: EngineOpts::default(),
                tenant_limits: None,
                expected: EngineCaps {
                    max_samples: EngineOpts::default().max_samples,
                    max_fetched_series: default_series,
                },
            },
            ProcessCapsCase {
                name: "tenant caps lower the process caps",
                process,
                tenant_limits: Some(tenant_caps(10, 3)),
                expected: EngineCaps {
                    max_samples: 10,
                    max_fetched_series: 3,
                },
            },
            ProcessCapsCase {
                name: "tenant caps cannot raise the process caps",
                process,
                tenant_limits: Some(tenant_caps(1_000, 70)),
                expected: EngineCaps {
                    max_samples: 100,
                    max_fetched_series: 7,
                },
            },
            ProcessCapsCase {
                name: "zero tenant caps keep the process caps",
                process,
                tenant_limits: Some(tenant_caps(0, 0)),
                expected: EngineCaps {
                    max_samples: 100,
                    max_fetched_series: 7,
                },
            },
            ProcessCapsCase {
                name: "a zero process series cap leaves the tenant cap",
                process: EngineOpts::default(),
                tenant_limits: Some(tenant_caps(0, 5)),
                expected: EngineCaps {
                    max_samples: EngineOpts::default().max_samples,
                    max_fetched_series: 5,
                },
            },
        ];
        for case in cases {
            let actual = engine_caps(case.process, case.tenant_limits);
            assert2::check!(
                actual.max_samples == case.expected.max_samples
                    && actual.max_fetched_series == case.expected.max_fetched_series,
                "{}",
                case.name
            );
        }
    }
}

mod cardinality_active_series_rejects_over_tenant_limit {
    use super::*;

    #[tokio::test]
    pub(crate) async fn cardinality_active_series_rejects_over_tenant_limit() {
        let limits = Limits {
            max_fetched_series_per_query: 1,
            ..Limits::default()
        };
        let response = limited_get(
            two_series_store(),
            limits,
            "/api/v1/cardinality/active_series",
        )
        .await;

        assert_execution_error(
            &response,
            |error| error.contains("series per query exceeded"),
            "active series",
        );
    }
}

mod discovery_rejects_label_counts_over_tenant_series_limit {
    use super::*;

    #[tokio::test]
    pub(crate) async fn discovery_rejects_label_counts_over_tenant_series_limit() {
        let uris = [
            "/api/v1/labels?start=0&end=1",
            "/api/v1/label/job/values?start=0&end=1",
        ];

        for uri in uris {
            assert_series_limit_rejected(uri).await;
        }
    }
}

mod promql_evaluation_rejects_series_over_tenant_limit {
    use super::*;

    #[tokio::test]
    pub(crate) async fn promql_evaluation_rejects_series_over_tenant_limit() {
        let uris = [
            "/api/v1/query?query=count(up)&time=0",
            "/api/v1/query_range?query=count(up)&start=0&end=60&step=60",
        ];

        for uri in uris {
            assert_series_limit_rejected(uri).await;
        }
    }
}

mod query_range_rejects_ranges_over_tenant_limit {
    use super::*;

    #[tokio::test]
    pub(crate) async fn query_range_rejects_ranges_over_tenant_limit() {
        let limits = Limits {
            max_query_length: minutes(1),
            ..Limits::default()
        };
        let response = limited_get(
            InMemoryMetricStore::new(),
            limits,
            "/api/v1/query_range?query=up&start=0&end=120&step=60",
        )
        .await;

        assert_execution_error(
            &response,
            |error| error.contains("query range too long"),
            "query_range",
        );
    }
}

mod query_rejects_lookback_over_tenant_limit {
    use super::*;

    #[tokio::test]
    pub(crate) async fn query_rejects_lookback_over_tenant_limit() {
        let limits = Limits {
            max_query_lookback: minutes(1),
            ..Limits::default()
        };
        let response = limited_get(
            InMemoryMetricStore::new(),
            limits,
            "/api/v1/query?query=up&time=0",
        )
        .await;

        assert_execution_error(
            &response,
            |error| error.contains("query lookback exceeded"),
            "query",
        );
    }
}

mod read_paths_admit_requests_when_limits_are_off {
    use super::*;

    #[tokio::test]
    pub(crate) async fn read_paths_admit_requests_when_limits_are_off() {
        let uris = [
            "/api/v1/query?query=count(up)&time=0",
            "/api/v1/query_range?query=count(up)&start=0&end=60&step=60",
            "/api/v1/labels?start=0&end=1",
            "/api/v1/label/job/values?start=0&end=1",
            "/api/v1/series?match[]=up&start=0&end=1",
            "/api/v1/query_exemplars?query=up&start=0&end=1",
        ];
        // Every cap turned off through a zero sentinel admits the request. So do the
        // default limits that a state without `with_query_limits` applies, because
        // an ordinary request is far inside them.
        let off = Limits {
            max_samples_per_query: 0,
            max_fetched_series_per_query: 0,
            max_query_lookback: minutes(0),
            max_query_length: minutes(0),
            ..Limits::default()
        };

        for uri in uris {
            for limits in [Some(off.clone()), None] {
                let state =
                    PrometheusApiState::new(Arc::new(two_series_store()), EngineOpts::default());
                let state = match limits {
                    Some(limits) => state.with_query_limits(OverridesProvider::new(limits)),
                    None => state,
                };

                let response = prometheus_router(Arc::new(state))
                    .oneshot(
                        Request::builder()
                            .uri(uri)
                            .header("x-scope-orgid", "tenant-a")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();

                assert2::assert!(response.status() == StatusCode::OK, "{uri}");
                let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert2::assert!(body["status"].as_str() == Some("success"), "{uri}");
            }
        }
    }
}

mod read_paths_answer_an_unusable_tenant_as_mimir_does {
    use super::*;

    /// Every row is an `X-Scope-OrgID` value that the pinned Mimir 2.16.1 image
    /// was sent on `/prometheus/api/v1/query`, `/prometheus/api/v1/series`,
    /// `/prometheus/api/v1/rules` and `/prometheus/config/v1/rules`, with the
    /// status and the whole body it answered. Mimir rejects the tenant before the
    /// Prometheus API handler runs, so the body is plain text with a line break
    /// and never the JSON error envelope. A Grafana datasource shows that text to
    /// its user.
    #[tokio::test]
    pub(crate) async fn read_paths_answer_an_unusable_tenant_as_mimir_does() {
        let too_long = "x".repeat(151);
        let too_long_and_bad = format!("{too_long}/");
        let unsupported = |tenant: &str, character: char| {
            format!("tenant ID '{tenant}' contains unsupported character '{character}'\n")
        };
        let too_many = |actual: usize| {
            format!("too many tenant IDs present in the request. max: 1 actual: {actual}\n")
        };
        struct TenantReadCase<'a> {
            name: &'a str,
            tenant: Option<&'a str>,
            status: StatusCode,
            body: String,
        }
        let rows = [
            TenantReadCase {
                name: "absent",
                tenant: None,
                status: StatusCode::UNAUTHORIZED,
                body: "no org id\n".into(),
            },
            TenantReadCase {
                name: "empty",
                tenant: Some(""),
                status: StatusCode::UNAUTHORIZED,
                body: "no org id\n".into(),
            },
            TenantReadCase {
                name: "separator",
                tenant: Some("a/b"),
                status: StatusCode::UNAUTHORIZED,
                body: unsupported("a/b", '/'),
            },
            TenantReadCase {
                name: "space",
                tenant: Some("a b"),
                status: StatusCode::UNAUTHORIZED,
                body: unsupported("a b", ' '),
            },
            TenantReadCase {
                name: "too long",
                tenant: Some(&too_long),
                status: StatusCode::UNAUTHORIZED,
                body: "tenant ID is too long: max 150 characters\n".into(),
            },
            TenantReadCase {
                name: "too long and unsupported",
                tenant: Some(&too_long_and_bad),
                status: StatusCode::UNAUTHORIZED,
                body: unsupported(&too_long_and_bad, '/'),
            },
            TenantReadCase {
                name: "dot dot",
                tenant: Some(".."),
                status: StatusCode::UNAUTHORIZED,
                body: "tenant ID is '.' or '..'\n".into(),
            },
            TenantReadCase {
                name: "two tenants",
                tenant: Some("a|b"),
                status: StatusCode::UNPROCESSABLE_ENTITY,
                body: too_many(2),
            },
            TenantReadCase {
                name: "three tenants",
                tenant: Some("a|b|c"),
                status: StatusCode::UNPROCESSABLE_ENTITY,
                body: too_many(3),
            },
            TenantReadCase {
                name: "an invalid second part",
                tenant: Some("a|b/c"),
                status: StatusCode::UNAUTHORIZED,
                body: unsupported("b/c", '/'),
            },
        ];
        let uris = [
            "/api/v1/query?query=up",
            "/prometheus/api/v1/query?query=up",
            "/api/v1/series?match[]=up",
            "/prometheus/api/v1/series?match[]=up",
            "/prometheus/api/v1/rules",
            "/prometheus/config/v1/rules",
        ];
        let router = prometheus_router(Arc::new(PrometheusApiState::new(
            Arc::new(two_series_store()),
            EngineOpts::default(),
        )));

        for uri in uris {
            for row in &rows {
                let mut request = Request::builder().uri(uri);
                if let Some(tenant) = row.tenant {
                    request = request.header("x-scope-orgid", tenant);
                }
                let response = router
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap();

                assert2::check!(response.status() == row.status, "{uri} {}", row.name);
                assert2::check!(
                    response
                        .headers()
                        .get(axum::http::header::CONTENT_TYPE)
                        .map(axum::http::HeaderValue::as_bytes)
                        == Some(&b"text/plain; charset=utf-8"[..]),
                    "{uri} {}",
                    row.name
                );
                let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
                assert2::check!(
                    String::from_utf8_lossy(&bytes) == row.body.as_str(),
                    "{uri} {}",
                    row.name
                );
            }
        }
    }
}

mod read_paths_reject_windows_over_tenant_limit {
    use super::*;

    #[tokio::test]
    pub(crate) async fn read_paths_reject_windows_over_tenant_limit() {
        let uris = [
            "/api/v1/labels?start=0&end=120",
            "/api/v1/label/job/values?start=0&end=120",
            "/api/v1/series?match[]=up&start=0&end=120",
            "/api/v1/query_exemplars?query=up&start=0&end=120",
        ];

        for uri in uris {
            let limits = Limits {
                max_query_length: minutes(1),
                ..Limits::default()
            };
            let response = limited_get(InMemoryMetricStore::new(), limits, uri).await;

            assert_execution_error(
                &response,
                |error| error.contains("query range too long"),
                uri,
            );
        }
    }
}

mod series_rejects_selected_series_over_tenant_limit {
    use super::*;

    #[tokio::test]
    pub(crate) async fn series_rejects_selected_series_over_tenant_limit() {
        let limits = Limits {
            max_fetched_series_per_query: 1,
            ..Limits::default()
        };
        let response = limited_get(
            two_series_store(),
            limits,
            "/api/v1/series?match[]=up&start=0&end=1",
        )
        .await;

        assert_execution_error(
            &response,
            |error| error.contains("series per query exceeded"),
            "series",
        );
    }
}
