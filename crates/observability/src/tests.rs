#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_units::{bytes, bytes_per_sec};

    use super::*;

    /// The compaction span belongs to the producer's trace, taken from the
    /// first record that actually carries a `traceparent`. A record without
    /// one sits first on purpose: selecting it instead extracts no context and
    /// leaves the batch in a trace of its own.
    #[test]
    fn a_compaction_batch_is_reparented_into_the_producers_trace() {
        use opentelemetry::trace::{TraceContextExt as _, TraceId, TracerProvider as _};
        use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        use tracing_subscriber::prelude::*;

        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("observability-compaction-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let record = |key: &str, value: &str| KafkaWalRecord {
                value: Vec::new(),
                partition: PartitionIndex(0),
                offset: Offset(0),
                timestamp_ms: None,
                headers: vec![KafkaWalHeader {
                    key: key.to_owned(),
                    value: Some(value.as_bytes().to_vec()),
                }],
            };
            let records = vec![
                record("tenant", "tenant-a"),
                record(
                    "traceparent",
                    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
                ),
            ];

            let span = tracing::info_span!("logs_compaction");
            set_remote_parent_from_wal_records(&span, &records);

            let context = span.context().span().span_context().clone();
            check!(
                context.trace_id()
                    == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").expect("a trace id")
            );
        });
    }

    /// A dynamic tenant index is built only when the querier serves every
    /// tenant *and* was pointed at a tenant index source. Either condition
    /// alone leaves it reading its own index.
    #[tokio::test]
    async fn a_dynamic_tenant_index_needs_both_no_tenant_and_a_tenant_index_source() {
        let dir = tempfile::tempdir().expect("a temp dir");
        write_log_index_manifest(dir.path(), &LabelIndex::default(), &BlockIndex::default())
            .expect("seed an empty local manifest");
        let configured = ConfiguredObjectStore {
            store: Arc::new(object_store::memory::InMemory::new()),
            prefix: ObjectPath::from("observability"),
        };
        write_tenant_log_index_manifest_to_object_store(
            configured.store.as_ref(),
            &ObjectPath::from("observability/index"),
            "tenant-a",
            &LabelIndex::default(),
            &BlockIndex::default(),
        )
        .await
        .expect("seed an empty tenant manifest");
        for (tenant, source, dynamic) in [
            (None, QuerierIndexSource::TenantObjectStoreManifest, true),
            (None, QuerierIndexSource::TenantObjectStoreShards, true),
            (None, QuerierIndexSource::LocalManifest, false),
            (
                Some("tenant-a"),
                QuerierIndexSource::TenantObjectStoreManifest,
                false,
            ),
        ] {
            let config = ServiceConfig {
                tenant: tenant.map(str::to_owned),
                querier_index_source: source,
                index_prefix: Some("index".to_owned()),
                data_root: dir.path().to_path_buf(),
                ..ServiceConfig::default()
            };
            let state = build_configured_querier_state(&config, &configured)
                .await
                .expect("the configuration is valid");
            check!(
                state.dynamic_index.is_some() == dynamic,
                "{tenant:?} with {source:?}"
            );
        }
    }

    /// `group_right` keeps the *right* side's series and carries the named
    /// labels over from the left. The match is by the `on` key, and a series
    /// the comparison rejects is dropped rather than kept at either value.
    #[test]
    fn a_group_right_comparison_keeps_the_right_series_it_matched() {
        let series = |labels: Value, value: &str| json!({"metric": labels, "value": [0, value]});
        let mut left = json!({"data": {"result": [
            series(json!({"app": "api", "env": "prod"}), "5")
        ]}});
        let right = json!({"data": {"result": [
            series(json!({"app": "api", "instance": "a"}), "1"),
            series(json!({"app": "api", "instance": "b"}), "9"),
            series(json!({"app": "worker", "instance": "c"}), "0")
        ]}});
        let matching = MetricVectorMatching::On {
            labels: vec!["app".to_owned()],
            group: Some(MetricVectorGroupModifier::Right(vec!["env".to_owned()])),
        };

        apply_metric_binary_comparison_to_loki_result(
            &mut left,
            &right,
            ComparisonOp::Greater,
            false,
            Some(&matching),
        );

        // Only the right series the left one beat survives, wearing the
        // right's own labels plus `env` carried over, and the left operand's
        // value. The `worker` series matches no left key at all.
        check!(
            left == json!({"data": {"result": [{
                "metric": {"app": "api", "instance": "a", "env": "prod"},
                "value": [0, "5"]
            }]}})
        );
    }

    /// A single authorized tenant takes the per-tenant path, and that is the
    /// only path applying `max_query_range` and the query-length limit -- the
    /// multi-tenant path's scalar shortcut checks neither. Routing one tenant
    /// through it would serve a query those limits refuse.
    #[tokio::test]
    async fn a_single_tenant_query_still_meets_the_configured_range_limit() {
        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_max_query_range(Time::from_nanos(1_000_000_000));
        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
        let params = QueryParams {
            query: "vector(1)".to_owned(),
            time: None,
            start: Some(0),
            end: Some(10_000_000_000),
            since: None,
            step: Some(1_000_000_000),
            interval: None,
            limit: None,
            direction: None,
            delay_for: None,
        };

        let error = execute_http_query(&state, &headers, params, QueryKind::Range)
            .await
            .expect_err("ten seconds is past the one-second maximum");
        check!(matches!(error, HttpQueryError::QueryRangeTooLarge { .. }));
    }

    /// `vector(` written inside a string literal is just text. Telling it from
    /// a real call needs the scanner to track exactly where the literal ends:
    /// a scanner that leaves early reads the text as a call, and one that
    /// never leaves swallows the code that follows.
    #[test]
    fn a_vector_call_inside_a_string_literal_is_not_a_signed_literal() {
        for (query, reported) in [
            ("vector(-1)", true),
            ("vector( +2)", true),
            ("vector(1)", false),
            // The call sits one character into the literal, so a scanner that
            // leaves the literal early lands on it and reports it.
            (
                r#"label_replace(vector(1), "dst", "Xvector( -1)", "s", "")"#,
                false,
            ),
            // And a real call after a literal is still reached, which a
            // scanner that never leaves one would never see.
            (
                r#"label_replace(vector(1), "a", "b", "s", "") + vector(-1)"#,
                true,
            ),
        ] {
            check!(
                signed_vector_function_literal_error(query).is_some() == reported,
                "{query}: {:?}",
                signed_vector_function_literal_error(query)
            );
        }
    }

    /// A delete request records its creation time in Unix *seconds*, the unit
    /// Loki's own API reports. Dividing rather than taking a remainder is what
    /// makes it a date at all -- a remainder is always under one second.
    #[test]
    fn a_created_delete_request_is_stamped_in_whole_seconds() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let state = CompactorDeleteState {
            delete_requests: SharedLogDeleteRequests::from_data_root(dir.path())
                .expect("an absent file is not an error"),
        };
        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));

        execute_create_delete_request(
            &state,
            &headers,
            Some(r#"query={job="api"}&start=1&end=2"#),
            &Bytes::new(),
        )
        .expect("the request is accepted");

        let requests = state
            .delete_requests
            .inner
            .lock()
            .expect("compactor delete state poisoned");
        check!(requests.requests.len() == 1);
        let created_at = requests.requests[0].created_at;
        // Any wall clock this code runs on is past 2020, and a sub-second
        // remainder can never reach that.
        check!(created_at > 1_600_000_000, "created_at was {created_at}");
    }

    /// Only a *missing* delete-request file reads as "no requests"; every
    /// other IO failure is an error. `refresh` then has to actually re-read
    /// that file -- a compactor that never picks up a request written by
    /// another process deletes nothing.
    #[test]
    fn delete_requests_reread_the_file_and_only_tolerate_it_being_absent() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let store = SharedLogDeleteRequests::from_data_root(dir.path())
            .expect("an absent file is not an error");
        check!(store.inner.lock().expect("not poisoned").next_id == 0);

        // Another process writes a request while this one holds the store.
        std::fs::write(
            log_delete_requests_path(dir.path()),
            r#"{"next_id":7,"requests":[{"tenant":"tenant-a","request_id":"r-1","query":"{job=\"api\"}","start_time":1,"end_time":2,"status":"received","created_at":3}]}"#,
        )
        .expect("write the request file");
        store.refresh().expect("the written file is readable");
        let inner = store.inner.lock().expect("not poisoned");
        check!(inner.next_id == 7);
        check!(inner.requests.len() == 1);
        check!(inner.requests[0].request_id == "r-1");
        drop(inner);

        // A directory in the file's place fails to read, and is not NotFound.
        let as_directory = dir.path().join("a-directory");
        std::fs::create_dir(&as_directory).expect("create the directory");
        let error = read_log_delete_requests(&as_directory)
            .expect_err("a directory is not a readable request file");
        check!(matches!(error, LogDeleteRequestStoreError::Io { .. }));
    }

    /// `loki_boltdb_shipper_compactor_running` is the one line in the status
    /// page that varies by component: it reads 1 for the compactor and 0 for
    /// everything else.
    #[tokio::test]
    async fn the_status_page_flags_only_the_compactor_as_running() {
        for (component, running) in [("compactor", 1), ("querier", 0), ("distributor", 0)] {
            let response = status_metrics(component);
            check!(response.status() == StatusCode::OK, "{component}");
            let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .expect("the response body is readable");
            let body = String::from_utf8(bytes.to_vec()).expect("utf-8");
            check!(
                body.contains(&format!(
                    "loki_boltdb_shipper_compactor_running {running}\n"
                )),
                "{component}: {body}"
            );
            check!(
                body.contains(&format!(
                    "krabka_observability_service_up{{component=\"{component}\"}} 1"
                )),
                "{component}"
            );
        }
    }

    /// A JSON push whose value is the empty string carries no timestamp of its
    /// own, so it is refused outright against the stale-sample window rather
    /// than dated to the epoch. With no window configured there is nothing to
    /// refuse it against.
    #[test]
    fn an_empty_json_value_is_refused_only_when_a_stale_window_is_set() {
        use krabka_units::hours;

        let labels = Labels::default();
        check!(validate_loki_empty_json_value_timestamp_window(&labels, None).is_ok());

        let error = validate_loki_empty_json_value_timestamp_window(&labels, Some(hours(1)))
            .expect_err("a configured window refuses an undated sample");
        check!(matches!(
            error,
            DistributorError::TimestampTooOldString {
                timestamp: "0001-01-01T00:00:00Z",
                ..
            }
        ));
    }

    /// `count_values` and `approx_topk` are both refused before a query is
    /// planned. The two predicates read different aggregation fields, so a
    /// query using one must not trip the other.
    #[test]
    fn count_values_and_approx_topk_are_recognised_apart_from_each_other() {
        for (query, count_values, approx_topk) in [
            (
                r#"count_values("status", rate({job="api"}[1m]))"#,
                true,
                false,
            ),
            (r#"approx_topk(3, rate({job="api"}[1m]))"#, false, true),
            (r#"sum(rate({job="api"}[1m]))"#, false, false),
            (r#"rate({job="api"}[1m])"#, false, false),
        ] {
            let parsed = parse_metric_query(query).expect(query);
            check!(
                metric_query_uses_count_values(&parsed) == count_values,
                "{query}"
            );
            check!(
                metric_query_uses_approx_topk(&parsed) == approx_topk,
                "{query}"
            );
        }
    }

    /// A JSON field's detected type comes from the JSON type itself, not from
    /// re-parsing its rendered text. Both integer widths count as `Int` --
    /// serde reports a negative one as `i64` only and a very large one as
    /// `u64` only, so either alone would demote the other to `Float`.
    #[test]
    fn json_fields_take_their_detected_type_from_the_json_value() {
        let line = r#"{"a_bool":true,"neg":-1,"huge":18446744073709551615,"real":1.5,"text":"hi","nothing":null}"#;
        let mut fields = BTreeMap::new();
        detect_json_fields(&mut fields, line);

        let stats = |ty, value: &str| DetectedFieldStats {
            ty,
            values: BTreeSet::from([value.to_owned()]),
            parsers: BTreeSet::from(["json"]),
        };
        let expected = BTreeMap::from([
            (
                "a_bool".to_owned(),
                stats(DetectedFieldType::Boolean, "true"),
            ),
            ("neg".to_owned(), stats(DetectedFieldType::Int, "-1")),
            (
                "huge".to_owned(),
                stats(DetectedFieldType::Int, "18446744073709551615"),
            ),
            ("real".to_owned(), stats(DetectedFieldType::Float, "1.5")),
            ("text".to_owned(), stats(DetectedFieldType::String, "hi")),
        ]);
        check!(fields == expected);
    }

    /// A range query's step is refused only when it is not positive; an absent
    /// one falls back to the range's own default rather than to zero.
    #[test]
    fn a_range_step_is_refused_only_when_it_is_not_positive() {
        let range = TimeRange::new(0, 60_000_000_000).unwrap();
        for (name, step, expected) in [
            (
                "a positive step is kept as given",
                Some(1_000_000_i64),
                Some(1_000_000_i64),
            ),
            ("zero is refused", Some(0), None),
            ("a negative step is refused", Some(-1), None),
            (
                "an absent step defaults off the range",
                None,
                Some(default_metric_range_step(range)),
            ),
        ] {
            check!(resolved_range_step(step, range).ok() == expected, "{name}");
        }
        check!(default_metric_range_step(range) > 0);
    }

    /// `max_query_range` and the `Loki` resolution cap are both inclusive: a
    /// query sitting exactly on the limit is served, and only the next
    /// nanosecond -- or the next point -- is refused. The boundary is the one
    /// input separating `>` from `>=` in either check.
    #[test]
    fn query_range_limits_admit_the_boundary_and_refuse_the_step_past_it() {
        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_max_query_range(Time::from_nanos(1_000_000));
        for (name, end_ns, allowed) in [
            ("exactly the limit", 1_000_000_i64, true),
            ("one nanosecond past it", 1_000_001, false),
        ] {
            let range = TimeRange::new(0, end_ns).unwrap();
            check!(
                validate_query_range_limit(&state, range).is_ok() == allowed,
                "{name}"
            );
        }

        // 11 000 points at a one-millisecond step is exactly the cap.
        for (name, end_ns, allowed) in [
            ("exactly the point cap", 11_000_000_000_i64, true),
            ("one point past the cap", 11_001_000_000, false),
        ] {
            let params = QueryParams {
                query: String::new(),
                time: None,
                start: None,
                end: None,
                since: None,
                step: Some(1_000_000),
                interval: None,
                limit: None,
                direction: None,
                delay_for: None,
            };
            let range = TimeRange::new(0, end_ns).unwrap();
            check!(
                validate_loki_query_range_resolution(&params, QueryKind::Range, range).is_ok()
                    == allowed,
                "{name}"
            );
        }
    }

    /// Both ends of the `Loki` ingestion window are strict comparisons: a
    /// timestamp exactly at the oldest or the newest acceptable value is
    /// accepted. That is the only input separating `<` from `<=`, and against
    /// a wall clock it is unreachable -- `now` advances between choosing the
    /// timestamp and the function reading it. Hence the `_at` seam, which
    /// takes `now` rather than reading it.
    #[test]
    fn the_loki_ingestion_window_accepts_its_own_boundaries() {
        use krabka_units::{hours, nanos};

        let now = 1_000_000_000_000_i64;
        let labels = Labels::default();
        let check_at = |timestamp: i64, max_age, grace| {
            super::validate_loki_timestamp_window_at(timestamp, now, &labels, max_age, grace)
        };
        let hour_ns = hours(1).nanos_i64();

        // Exactly at the oldest acceptable timestamp: accepted. One
        // nanosecond older: refused.
        check!(check_at(now - hour_ns, Some(hours(1)), None).is_ok());
        check!(check_at(now - hour_ns + 1, Some(hours(1)), None).is_ok());
        check!(check_at(now - hour_ns - 1, Some(hours(1)), None).is_err());

        // Exactly at the newest acceptable timestamp: accepted. One
        // nanosecond newer: refused.
        check!(check_at(now + hour_ns, None, Some(hours(1))).is_ok());
        check!(check_at(now + hour_ns - 1, None, Some(hours(1))).is_ok());
        check!(check_at(now + hour_ns + 1, None, Some(hours(1))).is_err());

        // A bound that is absent imposes nothing, and the two are
        // independent: an ancient timestamp passes with no max age, and a
        // far-future one passes with no grace period.
        check!(check_at(0, None, Some(hours(1))).is_ok());
        check!(check_at(i64::MAX / 2, Some(hours(1)), None).is_ok());
        check!(check_at(0, None, None).is_ok());
        check!(check_at(i64::MAX, None, None).is_ok());

        // A zero window admits only the instant itself.
        check!(check_at(now, Some(nanos(0)), Some(nanos(0))).is_ok());
        check!(check_at(now - 1, Some(nanos(0)), None).is_err());
        check!(check_at(now + 1, None, Some(nanos(0))).is_err());

        // The refusals name their own direction rather than sharing one error.
        check!(matches!(
            check_at(now - hour_ns - 1, Some(hours(1)), None),
            Err(DistributorError::TimestampTooOld { .. })
        ));
        check!(matches!(
            check_at(now + hour_ns + 1, None, Some(hours(1))),
            Err(DistributorError::TimestampTooNew { .. })
        ));
    }

    /// `ScalarSample` holds a rational, and its division normalises the sign
    /// so the denominator stays positive -- a negative divisor moves its sign
    /// to the numerator rather than leaving the pair in a form the rest of the
    /// type does not expect. Both signs are checked on each side.
    ///
    /// Division and power also refuse rather than produce a nonsense value:
    /// dividing by zero has no answer, and a negative base to a fractional
    /// power is NaN, which must not reach a series as a sample.
    #[test]
    fn scalar_division_and_power_refuse_what_has_no_answer() {
        let scalar = super::ScalarSample::new;
        let value =
            |result: Option<super::ScalarSample>| result.and_then(super::ScalarSample::to_f64);

        // Exact division, and a repeating fraction held as a rational rather
        // than rounded on the way in.
        check!(value(scalar(6, 1).divide(scalar(3, 1))) == Some(2.0));
        check!(value(scalar(1, 1).divide(scalar(3, 1))) == Some(1.0 / 3.0));
        check!(value(scalar(0, 1).divide(scalar(5, 1))) == Some(0.0));

        // Sign normalisation: a negative divisor, a negative dividend, and
        // both. Only the last returns to positive.
        check!(value(scalar(4, 1).divide(scalar(-2, 1))) == Some(-2.0));
        check!(value(scalar(-4, 1).divide(scalar(2, 1))) == Some(-2.0));
        check!(value(scalar(-4, 1).divide(scalar(-2, 1))) == Some(2.0));

        // Dividing by zero has no answer, whatever the dividend.
        check!(scalar(1, 1).divide(scalar(0, 1)).is_none());
        check!(scalar(0, 1).divide(scalar(0, 1)).is_none());
        check!(scalar(-1, 1).divide(scalar(0, 1)).is_none());

        // Powers, including the ones that are easy to get backwards.
        check!(value(scalar(2, 1).power(scalar(3, 1))) == Some(8.0));
        check!(
            value(scalar(3, 1).power(scalar(2, 1))) == Some(9.0),
            "not the other way round"
        );
        check!(value(scalar(2, 1).power(scalar(-1, 1))) == Some(0.5));
        check!(
            value(scalar(4, 1).power(scalar(1, 2))) == Some(2.0),
            "a fractional exponent"
        );
        check!(value(scalar(5, 1).power(scalar(0, 1))) == Some(1.0));

        // A negative base to a fractional power is NaN, which must be refused
        // rather than carried into a series as a sample.
        check!(scalar(-4, 1).power(scalar(1, 2)).is_none());
    }

    /// `parse_log_level_param` accepts the four levels and refuses everything
    /// else BY NAME, so the caller can tell "you sent a level I do not know"
    /// from "you sent no level". It returns on the first `log_level` it finds,
    /// which is what decides precedence when the handler merges two sources.
    #[test]
    fn a_log_level_parameter_names_why_it_was_refused() {
        let parse = |query: &str| super::parse_log_level_param(Some(query));

        for level in ["debug", "info", "warn", "error"] {
            check!(parse(&format!("log_level={level}")).ok().as_deref() == Some(level));
        }

        // The first occurrence wins, which the handler relies on.
        check!(parse("log_level=info&log_level=warn").ok().as_deref() == Some("info"));
        // And other parameters are skipped rather than ending the search.
        check!(parse("other=1&log_level=warn").ok().as_deref() == Some("warn"));
        check!(parse("log_level=warn&other=1").ok().as_deref() == Some("warn"));

        // Percent and plus escapes are decoded before matching, in the key as
        // well as the value.
        check!(parse("log%5Flevel=warn").ok().as_deref() == Some("warn"));

        // The two refusals are distinct: an unrecognised level names what was
        // sent, a missing one says the parameter was absent.
        check!(matches!(
            parse("log_level=verbose"),
            Err(HttpQueryError::InvalidQueryParameter {
                name: "log_level",
                ..
            })
        ));
        check!(
            matches!(
                parse("log_level="),
                Err(HttpQueryError::InvalidQueryParameter { .. }),
            ),
            "an empty value is an unrecognised level, not an absent parameter"
        );
        check!(matches!(
            parse("other=1"),
            Err(HttpQueryError::MissingQueryParameter("log_level"))
        ));
        check!(matches!(
            parse(""),
            Err(HttpQueryError::MissingQueryParameter("log_level"))
        ));
        check!(matches!(
            super::parse_log_level_param(None),
            Err(HttpQueryError::MissingQueryParameter("log_level"))
        ));

        // Case matters: the levels are lower-case spellings.
        check!(parse("log_level=DEBUG").is_err());
    }

    /// `log_level_post` accepts the level in a query string, a form body, or
    /// both. When both carry one the BODY wins, because the merged string puts
    /// it first and the parser returns on the first match -- an ordering that
    /// only shows when the two disagree.
    #[tokio::test]
    async fn a_log_level_post_prefers_the_body_over_the_query_string() {
        use axum::response::IntoResponse as _;

        let post = |query: Option<&str>, body: &str| {
            let query = query.map(str::to_string);
            let body = axum::body::Bytes::from(body.to_string());
            async move {
                let response = super::log_level_post(axum::extract::RawQuery(query), body)
                    .await
                    .into_response();
                let status = response.status();
                let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
                    .await
                    .expect("the response body is readable");
                (status, String::from_utf8(bytes.to_vec()).expect("utf-8"))
            }
        };

        // Either source alone.
        let (status, body) = post(Some("log_level=debug"), "").await;
        check!(status == axum::http::StatusCode::OK);
        check!(body.contains("Log level set to debug"));

        let (status, body) = post(None, "log_level=info").await;
        check!(status == axum::http::StatusCode::OK);
        check!(body.contains("Log level set to info"));

        // Both, disagreeing: the body wins.
        let (status, body) = post(Some("log_level=warn"), "log_level=info").await;
        check!(status == axum::http::StatusCode::OK);
        check!(
            body.contains("Log level set to info"),
            "the body's level, not the query string's: {body}"
        );

        // A body that carries no level at all, alongside a query string that
        // does. Every case above has the level in the body whenever the body is
        // non-empty, so the merge could have dropped the query string entirely
        // and they would all still pass.
        let (status, body) = post(Some("log_level=debug"), "other=1").await;
        check!(status == axum::http::StatusCode::OK);
        check!(
            body.contains("Log level set to debug"),
            "the query string supplies what the body lacks: {body}"
        );

        // An empty query string alongside a body is not a source.
        let (status, body) = post(Some(""), "log_level=error").await;
        check!(status == axum::http::StatusCode::OK);
        check!(body.contains("Log level set to error"));

        // Neither source, and an unrecognised level, are refused distinctly.
        let (status, body) = post(None, "").await;
        check!(status != axum::http::StatusCode::OK);
        check!(body.contains("unrecognized log level"));

        let (_, body) = post(Some("log_level=verbose"), "").await;
        check!(
            body.contains("verbose"),
            "the refusal names what was sent: {body}"
        );
    }

    /// The dynamic index caches hand back an entry only while it is fresh, and
    /// EVICT a stale one on the way past rather than leaving it to be found
    /// again. That eviction is the part worth pinning: a cache that returns
    /// None but keeps the entry grows without bound for any key queried after
    /// it expires.
    ///
    /// A zero TTL reaches the stale branch without sleeping -- any elapsed
    /// time at all is more than none. The boundary itself, an entry exactly at
    /// its TTL, is not reachable against a real clock and is not attempted.
    #[test]
    fn a_stale_dynamic_index_entry_is_evicted_rather_than_just_missed() {
        let fresh = super::DynamicIndexCache {
            cache_ttl: krabka_units::hours(1),
            shard_cache_ttl: krabka_units::hours(1),
            ..super::DynamicIndexCache::default()
        };
        let stale = super::DynamicIndexCache {
            cache_ttl: Time::ZERO,
            shard_cache_ttl: Time::ZERO,
            ..super::DynamicIndexCache::default()
        };
        let key = || super::DynamicIndexCacheKey::TenantManifest {
            tenant: "tenant".to_string(),
        };
        let shard_key = || super::DynamicShardIndexCacheKey {
            tenant: "tenant".to_string(),
            start_ns: 0,
            end_ns: 10,
        };
        let held = |cache: &super::DynamicIndexCache| {
            (
                cache.entries.lock().expect("the cache lock is held").len(),
                cache
                    .shard_indexes
                    .lock()
                    .expect("the shard cache lock is held")
                    .len(),
            )
        };

        // Within the TTL: found, and still held afterwards.
        fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
        fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
        check!(fresh.get(&key()).is_some());
        check!(fresh.get_shard_index(&shard_key()).is_some());
        check!(
            held(&fresh) == (1, 1),
            "a fresh hit leaves the entry in place"
        );

        // Past the TTL: a miss, and the entry is gone rather than merely
        // ignored -- so a second lookup finds nothing to evict.
        stale.insert(key(), LabelIndex::default(), BlockIndex::default());
        stale.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
        check!(held(&stale) == (1, 1), "inserted");
        check!(stale.get(&key()).is_none());
        check!(stale.get_shard_index(&shard_key()).is_none());
        check!(held(&stale) == (0, 0), "and evicted on the way past");

        // A key that was never inserted is a miss without disturbing anything.
        check!(
            fresh
                .get(&super::DynamicIndexCacheKey::TenantManifest {
                    tenant: "other".to_string(),
                })
                .is_none()
        );
        check!(held(&fresh) == (1, 1), "an absent key evicts nothing");

        // `clear` drops all three maps at once. It is what a configuration
        // reload calls, so with its body gone the querier keeps answering from
        // indexes built for the configuration it just replaced.
        fresh.insert(key(), LabelIndex::default(), BlockIndex::default());
        fresh.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
        fresh.insert_shard_ranges(
            super::DynamicShardRangesCacheKey {
                tenant: "tenant".to_string(),
            },
            0,
            Vec::new(),
        );
        check!(
            fresh
                .shard_ranges
                .lock()
                .expect("the shard range lock is held")
                .len()
                == 1,
            "the third map is populated too"
        );
        fresh.clear();
        check!(held(&fresh) == (0, 0), "cleared");
        check!(
            fresh
                .shard_ranges
                .lock()
                .expect("the shard range lock is held")
                .is_empty(),
            "including the shard ranges"
        );
        check!(fresh.get(&key()).is_none(), "and a lookup misses");

        // The two caches have their OWN durations -- five seconds and five
        // minutes by default -- so each must read its own. With both set alike
        // a lookup consulting the wrong one behaves identically, so here they
        // are opposites: the manifest expires immediately and the shard index
        // does not, then the reverse.
        let short_manifest = super::DynamicIndexCache {
            cache_ttl: Time::ZERO,
            shard_cache_ttl: krabka_units::hours(1),
            ..super::DynamicIndexCache::default()
        };
        short_manifest.insert(key(), LabelIndex::default(), BlockIndex::default());
        short_manifest.insert_shard_index(
            shard_key(),
            LabelIndex::default(),
            BlockIndex::default(),
        );
        check!(
            short_manifest.get(&key()).is_none(),
            "the manifest ttl is zero"
        );
        check!(
            short_manifest.get_shard_index(&shard_key()).is_some(),
            "but the shard ttl is an hour"
        );

        let short_shard = super::DynamicIndexCache {
            cache_ttl: krabka_units::hours(1),
            shard_cache_ttl: Time::ZERO,
            ..super::DynamicIndexCache::default()
        };
        short_shard.insert(key(), LabelIndex::default(), BlockIndex::default());
        short_shard.insert_shard_index(shard_key(), LabelIndex::default(), BlockIndex::default());
        check!(
            short_shard.get(&key()).is_some(),
            "the manifest ttl is an hour"
        );
        check!(
            short_shard.get_shard_index(&shard_key()).is_none(),
            "but the shard ttl is zero"
        );
    }

    /// `parse_decimal_sample_literal` reads a decimal literal as an EXACT
    /// rational rather than a float, which is the whole point: 0.1 has no
    /// float representation, and a sample that round-trips through one comes
    /// back as 0.100000000000000005551. The denominator is the power of ten
    /// the fraction needed, so the pair is returned unreduced.
    ///
    /// The exponent shifts that power either way, and the two directions take
    /// different branches -- a negative shift multiplies the numerator, a
    /// positive one raises the denominator -- so both are checked.
    ///
    /// Two mutations here are equivalent rather than untested. The branch test
    /// `decimal_places >= 0` could be `> 0`: at zero both paths raise ten to
    /// the zeroth and leave the numerator alone. And the early refusal of a
    /// second exponent marker is a fast path only -- `parse_decimal_sample_
    /// exponent` calls `parse::<i32>()`, which rejects anything containing an
    /// `e` anyway. Both are pinned by behaviour that cannot distinguish them.
    #[test]
    fn a_decimal_sample_literal_parses_to_an_exact_rational() {
        let parse = super::parse_decimal_sample_literal;

        // Whole numbers and plain decimals, unreduced.
        check!(parse("1") == Some((1, 1)));
        check!(parse("0") == Some((0, 1)));
        check!(parse("1.5") == Some((15, 10)), "unreduced: not (3, 2)");
        check!(parse("0.1") == Some((1, 10)), "exact, where a float is not");
        check!(parse("12.345") == Some((12_345, 1_000)));

        // Signs, on either spelling.
        check!(parse("-1.5") == Some((-15, 10)));
        check!(parse("+1.5") == Some((15, 10)));
        check!(parse("-0") == Some((0, 1)));

        // A missing side of the point is allowed as long as one side is there.
        check!(parse(".5") == Some((5, 10)));
        check!(parse("5.") == Some((5, 1)));
        check!(parse(".").is_none(), "but not both missing");

        // A positive exponent cancels decimal places and can go past them,
        // which switches branches: the numerator is scaled instead.
        check!(parse("1e3") == Some((1_000, 1)));
        check!(parse("1.5e2") == Some((150, 1)), "past the decimal places");
        check!(parse("1.5e1") == Some((15, 1)), "exactly cancelling them");
        check!(
            parse("1.25e1") == Some((125, 10)),
            "partially cancelling them"
        );

        // A negative exponent adds places, raising the denominator.
        check!(parse("1e-3") == Some((1, 1_000)));
        check!(parse("1.5e-2") == Some((15, 1_000)));
        check!(
            parse("15E-1") == Some((15, 10)),
            "the exponent marker is either case"
        );

        // Refusals: nothing to parse, or not a number.
        check!(parse("").is_none());
        check!(parse("-").is_none());
        check!(parse("abc").is_none());
        check!(
            parse("1.2.3").is_none(),
            "a second point is part of the fraction"
        );
        check!(parse("1e2e3").is_none(), "and a second exponent is refused");
        check!(parse("1e").is_none());
        check!(
            parse(" 1").is_none(),
            "no trimming: whitespace is not a digit"
        );
    }

    /// `metric_scalar_comparison_matches` compares a sample against a scalar,
    /// with a flag saying which side the scalar was written on. That flag only
    /// matters for the four ordered operators -- `1 > x` and `x > 1` disagree
    /// where `1 == x` and `x == 1` do not -- so every operator is checked at
    /// all three orderings AND on both sides.
    ///
    /// The two regex operators are always false here: a regex against a number
    /// is not a comparison `LogQL` can evaluate, and answering either way would
    /// silently filter samples on a predicate nobody wrote.
    #[test]
    fn a_scalar_comparison_answers_every_operator_from_both_sides() {
        use std::cmp::Ordering;

        use krabka_logql::ComparisonOp;

        let one = MetricValue::new(1, 1);
        let two = MetricValue::new(2, 1);
        let matches = |sample, op, scalar, scalar_on_left| {
            super::metric_scalar_comparison_matches(sample, op, scalar, scalar_on_left)
        };

        // (ordering of left against right, sample, scalar, scalar_on_left)
        let cases = [
            (Ordering::Less, one, two, false),
            (Ordering::Greater, one, two, true),
            (Ordering::Greater, two, one, false),
            (Ordering::Less, two, one, true),
            (Ordering::Equal, one, one, false),
            (Ordering::Equal, one, one, true),
        ];
        for (ordering, sample, scalar, scalar_on_left) in cases {
            let want = |op| match op {
                ComparisonOp::Equal => ordering == Ordering::Equal,
                ComparisonOp::NotEqual => ordering != Ordering::Equal,
                ComparisonOp::Greater => ordering == Ordering::Greater,
                ComparisonOp::GreaterEqual => ordering != Ordering::Less,
                ComparisonOp::Less => ordering == Ordering::Less,
                ComparisonOp::LessEqual => ordering != Ordering::Greater,
                ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
            };
            for op in [
                ComparisonOp::Equal,
                ComparisonOp::NotEqual,
                ComparisonOp::Greater,
                ComparisonOp::GreaterEqual,
                ComparisonOp::Less,
                ComparisonOp::LessEqual,
                ComparisonOp::RegexEqual,
                ComparisonOp::RegexNotEqual,
            ] {
                check!(
                    matches(sample, op, scalar, scalar_on_left) == want(op),
                    "{op:?} at {ordering:?} with scalar_on_left={scalar_on_left}"
                );
            }
        }

        // Spelled out for the case the table exists to protect: the scalar's
        // side changes the answer for an ordered operator and not for equality.
        check!(
            matches(one, ComparisonOp::Less, two, false),
            "x < 1 where x is smaller"
        );
        check!(
            !matches(one, ComparisonOp::Less, two, true),
            "but 1 < x is not"
        );
        check!(matches(one, ComparisonOp::Equal, one, false));
        check!(
            matches(one, ComparisonOp::Equal, one, true),
            "equality is side-blind"
        );
    }

    /// `page_groups` pages the rules response by group, resuming AFTER the
    /// token the client sent rather than at it -- resuming at it would return
    /// the same group forever. The token it hands back names the LAST group in
    /// the page, which is what makes the next request resume correctly.
    ///
    /// The two are checked against each other by walking a five-group list to
    /// exhaustion in pages of two: an off-by-one in either the resume or the
    /// token shows up as a repeated or skipped group rather than as a wrong
    /// count.
    #[test]
    fn paging_rule_groups_resumes_after_the_token_it_handed_back() {
        let groups = || {
            ["a", "b", "c", "d", "e"]
                .iter()
                .map(|name| super::PrometheusRuleGroupResponse {
                    token: (*name).to_string(),
                    value: serde_json::json!({"name": name}),
                })
                .collect::<Vec<_>>()
        };
        let page = |limit: Option<usize>, token: Option<&str>| {
            super::PrometheusRulesFilters {
                group_limit: limit,
                group_next_token: token.map(str::to_string),
                ..super::PrometheusRulesFilters::default()
            }
            .page_groups(groups())
        };
        let names = |page: &super::PrometheusRulesPage| {
            page.groups
                .iter()
                .map(|group| group["name"].as_str().expect("a name").to_string())
                .collect::<Vec<_>>()
        };

        // No limit returns everything, with nothing to resume from.
        let all = page(None, None).expect("no limit is valid");
        check!(names(&all) == vec!["a", "b", "c", "d", "e"]);
        check!(all.next_token.is_none());

        // Walk the list in pages of two. The token names the last group
        // returned, and the next page starts after it.
        let first = page(Some(2), None).expect("a first page");
        check!(names(&first) == vec!["a", "b"]);
        check!(
            first.next_token.as_deref() == Some("b"),
            "the LAST group of the page"
        );

        let second = page(Some(2), Some("b")).expect("a second page");
        check!(
            names(&second) == vec!["c", "d"],
            "resumes after b, not at it"
        );
        check!(second.next_token.as_deref() == Some("d"));

        // The final page is short and offers no token, because nothing follows.
        let third = page(Some(2), Some("d")).expect("a third page");
        check!(names(&third) == vec!["e"]);
        check!(third.next_token.is_none());

        // A page that exactly exhausts the list offers no token either: the
        // boundary is `>` and not `>=`, or a client would ask for an empty page.
        let exact = page(Some(5), None).expect("an exact page");
        check!(names(&exact) == vec!["a", "b", "c", "d", "e"]);
        check!(exact.next_token.is_none(), "nothing follows an exact fit");

        // A zero limit returns nothing and offers no token to resume from,
        // rather than a token that would never advance.
        let none = page(Some(0), None).expect("a zero limit is valid");
        check!(names(&none).is_empty());
        check!(none.next_token.is_none());

        // Resuming from the last group leaves an empty page.
        let past = page(Some(2), Some("e")).expect("resuming from the end");
        check!(names(&past).is_empty());

        // A token naming no group is a client error, not an empty page: it
        // usually means the group was deleted between requests.
        check!(page(Some(2), Some("nonsense")).is_err());
    }

    /// `count_loki_stream_result_hot_tail_lines` reports how many lines of a
    /// response came from the hot tail rather than from blocks. It is a
    /// MULTISET match-off: each hot record can account for one response line
    /// and no more, so two identical records admit two identical lines and a
    /// third line is attributed to the blocks.
    ///
    /// That counting-down is the whole design, and one record with one line
    /// cannot show it -- a plain membership test would agree.
    ///
    /// The key is built from the QUERY's output labels rather than the
    /// record's raw ones, which is why the response streams here carry a
    /// `detected_level` the records do not: the query synthesises it, so a
    /// response without it matches nothing at all.
    #[test]
    fn hot_tail_lines_are_matched_off_one_record_at_a_time() {
        use krabka_logql::StreamPlan;

        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        let plan = StreamPlan {
            tenant: "tenant".to_string(),
            time_range: krabka_blockstore::TimeRange::new(0, 100).expect("a valid range"),
            query: krabka_logql::parse_query("{app=\"api\"}").expect("the query parses"),
            fingerprints: BTreeSet::new(),
            blocks: Vec::new(),
        };
        let record = |tenant: &str, timestamp_ns, line: &str| super::WalLogRecord {
            tenant: tenant.to_string(),
            labels: labels.clone(),
            timestamp_ns,
            line: line.to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        };
        let response = |entries: &[(i64, &str)]| {
            serde_json::json!({
                "data": {"result": [{
                    "stream": {"app": "api", "detected_level": "unknown"},
                    "values": entries
                        .iter()
                        .map(|(ts, line)| serde_json::json!([ts.to_string(), line]))
                        .collect::<Vec<_>>(),
                }]}
            })
        };
        let open = super::CompactionFrontier::new(0);
        let counted = |value: &serde_json::Value, hot: &[super::WalLogRecord]| {
            super::count_loki_stream_result_hot_tail_lines(value, &plan, hot, &open)
        };

        // One record, one matching line.
        check!(counted(&response(&[(10, "a")]), &[record("tenant", 10, "a")]) == 1);

        // A line the hot tail does not hold came from the blocks.
        check!(counted(&response(&[(10, "b")]), &[record("tenant", 10, "a")]) == 0);
        check!(counted(&response(&[(20, "a")]), &[record("tenant", 10, "a")]) == 0);

        // The multiset: two identical records admit two identical lines, and
        // a third is attributed to the blocks rather than counted again.
        let twice = [record("tenant", 10, "a"), record("tenant", 10, "a")];
        check!(counted(&response(&[(10, "a")]), &twice) == 1);
        check!(counted(&response(&[(10, "a"), (10, "a")]), &twice) == 2);
        check!(
            counted(&response(&[(10, "a"), (10, "a"), (10, "a")]), &twice) == 2,
            "two records cannot account for three lines"
        );

        // Hot records are filtered before the match-off: another tenant, a
        // timestamp outside the plan's range, and one already compacted.
        check!(counted(&response(&[(10, "a")]), &[record("other", 10, "a")]) == 0);
        // Both ends of the range, since they are separate clauses. Past the
        // end is straightforward. BEFORE the start needs its own plan: with
        // this plan starting at zero, anything earlier is also behind the
        // compaction frontier and would be filtered by that instead, leaving
        // the start-of-range clause untested.
        check!(counted(&response(&[(200, "a")]), &[record("tenant", 200, "a")]) == 0);
        let later_plan = StreamPlan {
            time_range: krabka_blockstore::TimeRange::new(50, 100).expect("a valid range"),
            ..plan.clone()
        };
        check!(
            super::count_loki_stream_result_hot_tail_lines(
                &response(&[(10, "a")]),
                &later_plan,
                &[record("tenant", 10, "a")],
                &open,
            ) == 0,
            "before the range start, but after the frontier"
        );
        let compacted = super::CompactionFrontier::new(50);
        check!(
            super::count_loki_stream_result_hot_tail_lines(
                &response(&[(10, "a")]),
                &plan,
                &[record("tenant", 10, "a")],
                &compacted,
            ) == 0,
            "a compacted record is the blocks' line, not the hot tail's"
        );

        // Nothing on either side.
        check!(counted(&response(&[]), &[record("tenant", 10, "a")]) == 0);
        check!(counted(&response(&[(10, "a")]), &[]) == 0);
    }

    /// `append_matching_log_row` decides whether one row belongs in the
    /// response. Its first guard is three conditions or-ed as REJECTIONS --
    /// too early, too late, or not a series the plan wants -- so each rejects
    /// on its own against a row the other two accept.
    ///
    /// Both range bounds are inclusive, pinned by rows sitting exactly on
    /// each. A row whose series the label index cannot name is an ERROR rather
    /// than a skip: the plan asked for that series, so being unable to label
    /// it means the index disagrees with the plan.
    #[test]
    fn a_log_row_is_appended_only_when_the_plan_asked_for_it() {
        use krabka_logql::StreamPlan;

        let mut label_index = LabelIndex::default();
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        let known = label_index.insert_series("tenant", labels);
        let mut other = Labels::default();
        other.insert("app".to_string(), "web".to_string());
        let unwanted = label_index.insert_series("tenant", other);

        let plan = StreamPlan {
            tenant: "tenant".to_string(),
            time_range: krabka_blockstore::TimeRange::new(10, 90).expect("a valid range"),
            query: krabka_logql::parse_query("{app=\"api\"}").expect("the query parses"),
            fingerprints: [known].into_iter().collect(),
            blocks: Vec::new(),
        };
        let metadata = Labels::default();
        let appended = |fingerprint, timestamp_ns| {
            let mut streams = BTreeMap::new();
            let result = super::append_matching_log_row(
                &mut streams,
                &plan,
                &label_index,
                super::QueryRow {
                    fingerprint,
                    timestamp_ns,
                    line: "line",
                    structured_metadata: &metadata,
                },
                &[],
            );
            result.map(|()| streams.values().map(Vec::len).sum::<usize>())
        };

        // Inside the range, and a series the plan wants.
        check!(appended(known, 50).ok() == Some(1));
        // Exactly on each bound: both inclusive.
        check!(
            appended(known, 10).ok() == Some(1),
            "the start bound is inclusive"
        );
        check!(appended(known, 90).ok() == Some(1), "and so is the end");
        // One step outside each.
        check!(appended(known, 9).ok() == Some(0), "before the range");
        check!(appended(known, 91).ok() == Some(0), "after it");
        // A series the plan did not ask for, inside the range.
        check!(
            appended(unwanted, 50).ok() == Some(0),
            "not a wanted series"
        );

        // A fingerprint the label index cannot name is an error, not a skip --
        // but only once the row has passed the range and series filters, so a
        // nameless series the plan never wanted is still simply skipped.
        let nameless = 999_999_u64;
        check!(
            appended(nameless, 50).ok() == Some(0),
            "not wanted, so not named"
        );
        let mut wants_nameless = plan.clone();
        wants_nameless.fingerprints.insert(nameless);
        let mut streams = BTreeMap::new();
        check!(matches!(
            super::append_matching_log_row(
                &mut streams,
                &wants_nameless,
                &label_index,
                super::QueryRow {
                    fingerprint: nameless,
                    timestamp_ns: 50,
                    line: "line",
                    structured_metadata: &metadata,
                },
                &[],
            ),
            Err(super::QueryError::MissingSeriesLabels { .. })
        ));
    }

    /// `append_matching_hot_metric_record` folds one uncompacted WAL record
    /// into the samples for every evaluation window it belongs to. The window
    /// is HALF-OPEN -- `(end - range, end]` -- which is `rate()`'s own
    /// semantics: a record exactly at a window's end is inside it, and one
    /// exactly at the start belongs to the previous window instead. Without
    /// that, a record on a boundary would be counted twice.
    ///
    /// Records are also skipped for the wrong tenant or when already
    /// compacted, so the hot tier does not double-count what the blocks
    /// already hold. Each of those is broken alone against a record the rest
    /// accepts.
    #[tokio::test]
    async fn a_hot_metric_record_lands_in_every_window_that_contains_it() {
        use krabka_logql::parse_metric_query;

        let query = parse_metric_query("count_over_time({app=\"api\"}[10s])")
            .expect("the metric query parses");
        let record = |tenant: &str, timestamp_ns| {
            let mut labels = Labels::default();
            labels.insert("app".to_string(), "api".to_string());
            super::WalLogRecord {
                tenant: tenant.to_string(),
                labels,
                timestamp_ns,
                line: "line".to_string(),
                structured_metadata: BTreeMap::new(),
                position: None,
            }
        };
        let plan = krabka_logql::StreamPlan {
            tenant: "tenant".to_string(),
            time_range: krabka_blockstore::TimeRange::new(0, 1_000_000_000_000)
                .expect("a valid range"),
            query: query.stream.clone(),
            fingerprints: BTreeSet::new(),
            blocks: Vec::new(),
        };
        let range_ns = 10_000_000_000_i64;
        // Two windows, ten seconds apart, so a record can land in one, both,
        // or neither.
        let eval_times = [20_000_000_000_i64, 30_000_000_000_i64];

        let windows_hit = |record: &super::WalLogRecord, frontier: &super::CompactionFrontier| {
            let mut samples = BTreeMap::new();
            super::append_matching_hot_metric_record(
                &mut samples,
                &plan,
                record,
                frontier,
                super::MetricWindow {
                    query: &query,
                    eval_times: &eval_times,
                    range_ns,
                    delete_filters: &[],
                },
            )
            .expect("the record folds in");
            samples
                .values()
                .flat_map(BTreeMap::keys)
                .copied()
                .collect::<BTreeSet<_>>()
        };
        let open = super::CompactionFrontier::new(0);

        // Exactly at a window's end: inside that window.
        check!(
            windows_hit(&record("tenant", 20_000_000_000), &open) == [20_000_000_000].into(),
            "a record at the window end is inside it"
        );
        // Exactly at a window's start: NOT in it -- it belongs to the window
        // before, which is not being evaluated here.
        check!(
            windows_hit(&record("tenant", 10_000_000_000), &open).is_empty(),
            "a record at the window start belongs to the previous window"
        );
        // One nanosecond past the start is inside.
        check!(windows_hit(&record("tenant", 10_000_000_001), &open) == [20_000_000_000].into());
        // In the overlap of neither window.
        check!(windows_hit(&record("tenant", 5_000_000_000), &open).is_empty());
        // Inside the second window only.
        check!(windows_hit(&record("tenant", 25_000_000_000), &open) == [30_000_000_000].into());

        // A record for another tenant is skipped even when it is in range.
        check!(windows_hit(&record("other", 20_000_000_000), &open).is_empty());

        // A record the blocks already hold is skipped, so the hot tier does
        // not double-count it.
        let compacted = super::CompactionFrontier::new(21_000_000_000);
        check!(
            windows_hit(&record("tenant", 20_000_000_000), &compacted).is_empty(),
            "already compacted"
        );

        // An `offset` shifts the window BACK in time. Without one the offset
        // is zero and adding it reads the same as subtracting, so this needs
        // its own query: offset 5s puts the window for eval time 20s at
        // (5s, 15s], where a record at exactly 15s is inside. Added instead,
        // the window would be (15s, 25s] and 15s would fall outside it.
        let offset_query = parse_metric_query("count_over_time({app=\"api\"}[10s] offset 5s)")
            .expect("the offset query parses");
        let mut samples = BTreeMap::new();
        super::append_matching_hot_metric_record(
            &mut samples,
            &plan,
            &record("tenant", 15_000_000_000),
            &open,
            super::MetricWindow {
                query: &offset_query,
                eval_times: &[20_000_000_000],
                range_ns,
                delete_filters: &[],
            },
        )
        .expect("the record folds in");
        // The INNER keys, not whether `samples` has anything in it: the outer
        // entry for the series is created as soon as the record matches the
        // query, before any window is considered, so an empty-map check would
        // pass whatever the windows decided.
        check!(
            samples
                .values()
                .flat_map(BTreeMap::keys)
                .copied()
                .collect::<BTreeSet<_>>()
                == [20_000_000_000].into(),
            "the offset moves the window back, not forward"
        );
    }

    /// `parse_label_replace_metric_binary_expression` recognises a binary
    /// expression where EITHER side is a `label_replace(...)`, and reports
    /// which kind of binary it is. The three kinds are tried in order --
    /// arithmetic, comparison, set -- and each must produce its own variant,
    /// since they are handled by different evaluators downstream.
    ///
    /// Either side qualifying is the point: a `label_replace` on the right
    /// alone is just as much this shape as one on the left, and the two go
    /// through the same `||`.
    #[test]
    fn a_label_replace_binary_expression_names_its_own_kind() {
        use super::LabelReplaceMetricBinaryExpression as Expression;

        let parse = super::parse_label_replace_metric_binary_expression;
        let replace = r#"label_replace(up,"a","b","c","d")"#;

        // Arithmetic, with the label_replace on each side in turn.
        check!(matches!(
            parse(&format!("{replace} + up")),
            Some(Expression::Arithmetic { .. })
        ));
        check!(
            matches!(
                parse(&format!("up + {replace}")),
                Some(Expression::Arithmetic { .. })
            ),
            "on the right is equally this shape"
        );

        // Comparison and set each get their own variant.
        check!(matches!(
            parse(&format!("{replace} > up")),
            Some(Expression::Comparison { .. })
        ));
        check!(matches!(
            parse(&format!("{replace} and up")),
            Some(Expression::Set { .. })
        ));

        // The operands are carried through trimmed, not with the whitespace
        // the split left on them.
        let Some(Expression::Arithmetic { left, right, .. }) = parse(&format!("{replace}  +  up"))
        else {
            panic!("an arithmetic expression");
        };
        check!(left == replace, "the left operand is trimmed");
        check!(right == "up", "and so is the right");

        // The operator is carried through, not assumed. Subtraction is used
        // because it is not the variant a collapsed arm would default to.
        let Some(Expression::Arithmetic { op, .. }) = parse(&format!("{replace} - up")) else {
            panic!("an arithmetic expression");
        };
        check!(op == krabka_logql::MetricScalarArithmeticOp::Subtract);
        let Some(Expression::Comparison { op, .. }) = parse(&format!("{replace} < up")) else {
            panic!("a comparison expression");
        };
        check!(op == krabka_logql::ComparisonOp::Less);

        // A binary expression with no label_replace on either side is not this
        // shape, and is parsed elsewhere.
        check!(parse("up + down").is_none());
        check!(parse("up > down").is_none());
        check!(parse("up and down").is_none());

        // Nor is a bare label_replace with no binary operator at all.
        check!(parse(replace).is_none());
        check!(parse("").is_none());
    }

    /// `sample_time_bucket` floors a sample onto the step grid measured FROM
    /// the query's start, not from the epoch. A start that is not itself a
    /// multiple of the step is what shows that: with start 0 the two are the
    /// same arithmetic, and every bucket would look right.
    ///
    /// A sample before the start clamps to the start rather than producing a
    /// bucket before the window began. The `<=` in that guard could be `<`
    /// without changing any answer -- at exactly the start the arithmetic
    /// yields the start anyway -- so relaxing it is an equivalent mutation.
    /// The guard as a whole is not: a sample below the start would otherwise
    /// floor to a negative offset.
    #[test]
    fn a_sample_buckets_onto_the_grid_measured_from_the_query_start() {
        let bucket = super::sample_time_bucket;
        // 1_000 is deliberately not a multiple of 300.
        let (start, step) = (1_000_i64, 300_i64);

        // The grid runs 1000, 1300, 1600 -- not 900, 1200, 1500, which is what
        // flooring from the epoch would give.
        check!(
            bucket(1_000, start, step) == 1_000,
            "the start is its own bucket"
        );
        check!(bucket(1_001, start, step) == 1_000);
        check!(bucket(1_299, start, step) == 1_000, "one short of the next");
        check!(bucket(1_300, start, step) == 1_300, "exactly on the next");
        check!(bucket(1_301, start, step) == 1_300);
        check!(bucket(1_900, start, step) == 1_900, "three steps along");
        check!(bucket(2_000, start, step) == 1_900);

        // At or before the start, clamped.
        check!(bucket(999, start, step) == 1_000);
        check!(bucket(0, start, step) == 1_000);
        check!(bucket(-1_000, start, step) == 1_000);

        // A start of zero is the degenerate case where flooring from the start
        // and from the epoch agree -- pinned so the distinction above is not
        // mistaken for the only behaviour.
        check!(bucket(700, 0, step) == 600);
        check!(bucket(600, 0, step) == 600);
    }

    /// `metadata_fingerprints_in_time_range` collects the series present in a
    /// window, and does something worth pinning when a block's FILE is gone:
    /// it falls back to the fingerprints the index already records for that
    /// block, rather than failing the whole request. The index knows which
    /// series a block held, so a deleted or not-yet-fetched file degrades to a
    /// coarser answer instead of no answer.
    ///
    /// The fallback is coarser in a specific way: it ignores the time range,
    /// since without the rows there is nothing to filter. The test shows that
    /// by asking for a window the missing block's rows would have fallen
    /// outside of, and still getting its series back.
    #[tokio::test]
    async fn missing_metadata_blocks_fall_back_to_their_indexed_fingerprints() {
        use krabka_blockstore::{BlockKey, LogRow, TimeRange, write_log_block};

        let dir = tempfile::tempdir().expect("a temp dir");
        let range = |start_ns, end_ns| TimeRange::new(start_ns, end_ns).expect("a valid range");
        let row = |fingerprint: u64, timestamp_ns| LogRow {
            series_fingerprint: fingerprint,
            timestamp_ns,
            line: "line".to_string(),
            structured_metadata: BTreeMap::new(),
        };

        // One block that exists on disk, holding two series at 10 and 90.
        let present_key = BlockKey::new("tenant", 0, 0, 0, range(0, 100));
        let present = write_log_block(dir.path(), &present_key, vec![row(1, 10), row(2, 90)])
            .expect("the block writes");

        // One block the index knows about whose file was never written.
        let missing_key = BlockKey::new("tenant", 0, 1, 1, range(0, 100));
        let missing = krabka_blockstore::BlockDescriptor::new(
            missing_key,
            [7_u64, 8_u64].into_iter().collect(),
        );

        let mut index = BlockIndex::default();
        index.insert(present);
        index.insert(missing);
        let state = super::QuerierState::new(dir.path(), LabelIndex::default(), index);

        let series = |time_range| {
            let state = &state;
            async move {
                super::metadata_fingerprints_in_time_range(state, "tenant", time_range)
                    .await
                    .expect("the metadata reads")
            }
        };

        // The whole window: both real series, plus the missing block's two.
        check!(
            series(range(0, 100)).await == [1_u64, 2, 7, 8].into_iter().collect(),
            "the indexed fingerprints stand in for the unreadable block"
        );

        // A narrow window excludes the row at 90 from the block that EXISTS,
        // but the missing block still contributes both of its series -- the
        // fallback cannot filter by time.
        check!(
            series(range(0, 50)).await == [1_u64, 7, 8].into_iter().collect(),
            "the fallback ignores the range it cannot check"
        );

        // A window ending exactly on a row keeps it: both bounds are
        // inclusive, and no other range here puts a row on its edge.
        check!(
            series(range(0, 90)).await == [1_u64, 2, 7, 8].into_iter().collect(),
            "the row at 90 is inside a window ending at 90"
        );
        check!(
            series(range(10, 89)).await == [1_u64, 7, 8].into_iter().collect(),
            "and outside one ending at 89"
        );

        // A window matching no block at all yields nothing.
        check!(series(range(1_000, 2_000)).await.is_empty());
    }

    /// `count_index_stats_entries` counts the rows a plan would actually read:
    /// those whose series is in the plan AND whose timestamp falls inside its
    /// range. All three conditions are and-ed, so each is broken alone against
    /// a row the other two accept.
    ///
    /// Both bounds are INCLUSIVE here, unlike `count_stream_map_lines` whose
    /// end is exclusive. The two count different things -- one the rows on
    /// disk, the other the lines already returned -- so the difference is
    /// deliberate, and each is pinned at its own boundary.
    #[tokio::test]
    async fn counting_index_stats_reads_only_the_rows_a_plan_would() {
        use krabka_blockstore::{BlockKey, LogRow, TimeRange, write_log_block};
        use krabka_logql::{StreamPlan, StreamQuery};

        let dir = tempfile::tempdir().expect("a temp dir");
        let key = BlockKey::new(
            "tenant",
            0,
            0,
            0,
            TimeRange::new(0, 100).expect("a valid range"),
        );
        let row = |fingerprint: u64, timestamp_ns| LogRow {
            series_fingerprint: fingerprint,
            timestamp_ns,
            line: "line".to_string(),
            structured_metadata: BTreeMap::new(),
        };
        // Two series, and timestamps sitting on and either side of the bounds
        // the plan will use.
        let descriptor = write_log_block(
            dir.path(),
            &key,
            vec![
                row(1, 9),
                row(1, 10),
                row(1, 50),
                row(1, 90),
                row(1, 91),
                row(2, 50),
            ],
        )
        .expect("the block writes");

        let state =
            super::QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default());
        let plan = |fingerprints: &[u64], start_ns, end_ns| StreamPlan {
            tenant: "tenant".to_string(),
            time_range: TimeRange::new(start_ns, end_ns).expect("a valid range"),
            query: StreamQuery {
                matchers: Vec::new(),
                pipeline: Vec::new(),
            },
            fingerprints: fingerprints.iter().copied().collect(),
            blocks: vec![descriptor.clone()],
        };
        let count = |plan: StreamPlan| {
            let state = &state;
            async move {
                super::count_index_stats_entries(state, &plan)
                    .await
                    .expect("the block reads")
            }
        };

        // Series 1 has rows at 9, 10, 50, 90 and 91. Within 10..=90 that is
        // three: the ones at 9 and 91 fall outside, and series 2's row is a
        // different series.
        check!(
            count(plan(&[1], 10, 90)).await == 3,
            "both bounds inclusive"
        );

        // Each bound moved in by one drops the row sitting exactly on it,
        // which is what makes the bounds observably inclusive.
        check!(
            count(plan(&[1], 11, 90)).await == 2,
            "the row at 10 is dropped"
        );
        check!(count(plan(&[1], 10, 89)).await == 2, "and the row at 90");

        // The series filter, alone.
        check!(count(plan(&[2], 0, 100)).await == 1, "only series 2's row");
        check!(
            count(plan(&[1, 2], 0, 100)).await == 6,
            "both series, whole range"
        );
        check!(count(plan(&[], 0, 100)).await == 0, "no series, no rows");

        // A range that excludes everything, and a plan with no blocks.
        check!(count(plan(&[1, 2], 200, 300)).await == 0);
        let mut empty = plan(&[1], 0, 100);
        empty.blocks.clear();
        check!(count(empty).await == 0, "no blocks, nothing to read");

        // Two blocks are SUMMED. With one block, accumulating and replacing
        // give the same answer, so a second block is what makes the running
        // total observable.
        let second_key = BlockKey::new(
            "tenant",
            0,
            1,
            1,
            TimeRange::new(0, 100).expect("a valid range"),
        );
        let second = write_log_block(dir.path(), &second_key, vec![row(1, 20), row(1, 30)])
            .expect("the second block writes");
        let mut both = plan(&[1], 0, 100);
        both.blocks.push(second);
        check!(
            count(both).await == 7,
            "five rows in the first block and two in the second"
        );
    }

    /// The three per-query limits share a shape: unset means no limit, a query
    /// exactly at the limit is allowed, and one unit over is refused. Each is
    /// checked at all three points, because `>` and `>=` differ only at the
    /// boundary and "unset" is a third answer distinct from a limit of zero.
    ///
    /// They are tested together because they are parallel by design and a
    /// reader comparing them should see the same three cases each; a mutant
    /// swapping one limit's comparison for another's is caught by their
    /// carrying different values.
    #[test]
    fn every_per_query_limit_admits_exactly_its_boundary() {
        use krabka_blockstore::{BlockDescriptor, BlockKey, TimeRange};
        use krabka_logql::{StreamPlan, StreamQuery};

        let plan = |fingerprints: usize, block_bytes: &[u32]| StreamPlan {
            tenant: "tenant".to_string(),
            time_range: TimeRange::new(0, 10).expect("a valid range"),
            query: StreamQuery {
                matchers: Vec::new(),
                pipeline: Vec::new(),
            },
            fingerprints: (0..u64::try_from(fingerprints).expect("a small count")).collect(),
            blocks: block_bytes
                .iter()
                .enumerate()
                .map(|(index, size)| {
                    BlockDescriptor::new_with_size(
                        BlockKey::new(
                            "tenant",
                            0,
                            i64::try_from(index).expect("a small index"),
                            i64::try_from(index).expect("a small index"),
                            TimeRange::new(0, 10).expect("a valid range"),
                        ),
                        BTreeSet::new(),
                        krabka_units::bytes(*size),
                    )
                })
                .collect(),
        };
        let base = || super::QuerierState::new(".", LabelIndex::default(), BlockIndex::default());

        // Series: three fingerprints against a limit of three, then two.
        check!(
            super::validate_query_series_limit(&base(), &plan(3, &[])).is_ok(),
            "unset"
        );
        check!(
            super::validate_query_series_limit(&base().with_max_query_series(3), &plan(3, &[]))
                .is_ok(),
            "exactly at the limit"
        );
        check!(
            super::validate_query_series_limit(&base().with_max_query_series(2), &plan(3, &[]))
                .is_err(),
            "one over"
        );

        // Bytes: the planned total is SUMMED across blocks, so two blocks are
        // used -- one block cannot tell a sum from a maximum.
        let two_blocks = plan(0, &[40, 60]);
        check!(
            super::validate_query_bytes_limit(&base(), &two_blocks).is_ok(),
            "unset"
        );
        check!(
            super::validate_query_bytes_limit(
                &base().with_max_query_read(krabka_units::bytes(100)),
                &two_blocks,
            )
            .is_ok(),
            "exactly at the summed limit"
        );
        check!(
            super::validate_query_bytes_limit(
                &base().with_max_query_read(krabka_units::bytes(99)),
                &two_blocks,
            )
            .is_err(),
            "one byte over"
        );

        // Length: measured in bytes of the query text.
        let query = "{app=\"api\"}";
        check!(
            super::validate_query_length_limit(&base(), query).is_ok(),
            "unset"
        );
        check!(
            super::validate_query_length_limit(
                &base().with_max_query_length(krabka_units::bytes(
                    u32::try_from(query.len()).expect("a short query")
                )),
                query,
            )
            .is_ok(),
            "exactly at the limit"
        );
        check!(
            super::validate_query_length_limit(
                &base().with_max_query_length(krabka_units::bytes(
                    u32::try_from(query.len()).expect("a short query") - 1
                )),
                query,
            )
            .is_err(),
            "one byte over"
        );

        // Each refusal names its own limit rather than a shared message.
        check!(matches!(
            super::validate_query_series_limit(&base().with_max_query_series(2), &plan(3, &[])),
            Err(HttpQueryError::QuerySeriesTooLarge { .. })
        ));
        check!(matches!(
            super::validate_query_bytes_limit(
                &base().with_max_query_read(krabka_units::bytes(99)),
                &two_blocks,
            ),
            Err(HttpQueryError::QueryBytesTooLarge { .. })
        ));
        check!(matches!(
            super::validate_query_length_limit(
                &base().with_max_query_length(krabka_units::bytes(1)),
                query,
            ),
            Err(HttpQueryError::QueryLengthTooLarge { .. })
        ));
    }

    /// `validate_loki_volume_query_range_limit` caps a volume query's span at
    /// 30 days and a bit. The cap is exclusive of nothing -- a range exactly at
    /// the limit is allowed and one nanosecond more is not, which is the pair
    /// separating `>` from `>=`.
    ///
    /// A span that overflows an i64 subtraction is refused too, and reports the
    /// widest length rather than a negative one: a wrapped subtraction would
    /// otherwise report a query "shorter" than the limit and let it through.
    #[test]
    fn a_volume_query_range_is_capped_at_its_limit_exactly() {
        use krabka_blockstore::TimeRange;

        let max_ns = super::LOKI_VOLUME_MAX_QUERY_RANGE.nanos_i64();
        let range = |start_ns, end_ns| {
            super::validate_loki_volume_query_range_limit(
                TimeRange::new(start_ns, end_ns).expect("a valid range"),
            )
        };

        check!(range(0, 0).is_ok(), "an empty range is within any limit");
        check!(range(0, max_ns).is_ok(), "exactly at the limit");
        check!(range(1_000, 1_000 + max_ns).is_ok(), "wherever it starts");
        check!(range(0, max_ns + 1).is_err(), "one nanosecond over");

        // The error names how long the query actually was, so the client can
        // see by how much it missed.
        let error = range(0, max_ns + 1).expect_err("over the limit");
        check!(matches!(
            error,
            HttpQueryError::LokiQueryRangeTooLarge { .. }
        ));

        // A span that cannot be subtracted without overflowing is refused
        // rather than wrapping to a small positive number.
        check!(range(i64::MIN, i64::MAX).is_err(), "an overflowing span");
    }

    /// `validate_native_timestamp_ns` refuses a negative timestamp and returns
    /// the value otherwise. Zero is the boundary -- the Unix epoch is a real
    /// instant, so it is accepted, which is what separates `< 0` from `<= 0`.
    #[test]
    fn a_native_timestamp_may_be_the_epoch_but_not_before_it() {
        let validate = |timestamp_ns| {
            super::validate_native_timestamp_ns(timestamp_ns, timestamp_ns.to_string())
        };

        check!(validate(0).ok() == Some(0), "the epoch is a real instant");
        check!(validate(1).ok() == Some(1));
        check!(validate(i64::MAX).ok() == Some(i64::MAX));
        check!(validate(-1).is_err());
        check!(validate(i64::MIN).is_err());

        // The refusal carries the value it refused, so a log line names the
        // timestamp that was wrong rather than only that one was.
        let error = validate(-42).expect_err("negative is refused");
        check!(error.to_string().contains("-42"), "got: {error}");
    }

    /// `count_stream_map_lines` counts entries across every stream, optionally
    /// stopping before a timestamp. The bound is EXCLUSIVE, so an entry landing
    /// exactly on it is not counted -- that is the one input separating `<`
    /// from `<=`, and it matters because the same instant is the next page's
    /// first entry and would otherwise be counted twice.
    ///
    /// An entry whose timestamp will not parse IS counted. It is a line that
    /// exists, and a count used for paging must not under-report it.
    #[test]
    fn counting_stream_lines_stops_before_its_bound_but_keeps_odd_entries() {
        let streams = |entries: &[(&str, &[&str])]| {
            entries
                .iter()
                .map(|(app, timestamps)| {
                    let mut labels = Labels::default();
                    labels.insert("app".to_string(), (*app).to_string());
                    (
                        labels,
                        timestamps
                            .iter()
                            .map(|ts| [(*ts).to_string(), "line".to_string()])
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let count = super::count_stream_map_lines;

        // Unbounded: every entry across every stream.
        let two = streams(&[("api", &["1", "2", "3"]), ("web", &["4", "5"])]);
        check!(count(&two, None) == 5, "summed across streams");

        // Bounded, exclusive: 3 is counted, 4 is not.
        check!(count(&two, Some(4)) == 3);
        check!(count(&two, Some(5)) == 4, "the bound itself is excluded");
        check!(count(&two, Some(6)) == 5);
        check!(count(&two, Some(1)) == 0, "nothing before the first");

        // An unparseable timestamp is counted, bounded or not.
        let odd = streams(&[("api", &["1", "nonsense", "9"])]);
        check!(count(&odd, None) == 3);
        check!(count(&odd, Some(2)) == 2, "1 and the odd entry, but not 9");

        // Nothing to count.
        check!(count(&BTreeMap::new(), None) == 0);
        check!(count(&streams(&[("api", &[])]), None) == 0);
    }

    /// `apply_loki_stream_interval` thins a stream so consecutive entries are at
    /// least `interval` apart, keeping the first of each window. The entries
    /// straddle the boundary deliberately: one exactly AT the next allowed
    /// timestamp must be kept, since the comparison is `<` and not `<=`.
    ///
    /// An entry whose timestamp will not parse is KEPT rather than dropped --
    /// thinning is a display convenience, and silently discarding a line
    /// because its timestamp is odd would lose data the user asked for.
    #[test]
    fn a_loki_stream_interval_keeps_the_first_entry_of_each_window() {
        let stream = |timestamps: &[&str]| {
            serde_json::json!({
                "data": {"result": [{
                    "stream": {"app": "api"},
                    "values": timestamps
                        .iter()
                        .map(|ts| serde_json::json!([ts, "line"]))
                        .collect::<Vec<_>>(),
                }]}
            })
        };
        let kept = |mut value: serde_json::Value, interval| {
            super::apply_loki_stream_interval(&mut value, interval);
            value
                .pointer("/data/result")
                .and_then(serde_json::Value::as_array)
                .map(|streams| {
                    streams
                        .iter()
                        .flat_map(|s| s["values"].as_array().cloned().unwrap_or_default())
                        .map(|entry| entry[0].as_str().expect("a timestamp").to_string())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };

        // Ten apart: the first is kept, then everything until ten past it.
        // 10 lands exactly on the boundary and is kept.
        check!(
            kept(stream(&["0", "5", "10", "15", "20"]), Some(10)) == vec!["0", "10", "20"],
            "an entry exactly at the boundary is kept"
        );
        check!(
            kept(stream(&["0", "9"]), Some(10)) == vec!["0"],
            "one short is dropped"
        );

        // No interval, and a zero interval, both leave the stream alone.
        check!(kept(stream(&["0", "1", "2"]), None) == vec!["0", "1", "2"]);
        check!(kept(stream(&["0", "1", "2"]), Some(0)) == vec!["0", "1", "2"]);

        // The zero-interval short circuit only shows on DESCENDING timestamps,
        // which is how Loki returns log entries. Without it a zero interval
        // sets the next allowed timestamp to the current one, and every later
        // entry compares as earlier and is dropped.
        check!(
            kept(stream(&["2", "1", "0"]), Some(0)) == vec!["2", "1", "0"],
            "a zero interval thins nothing, even newest-first"
        );

        // An unparseable timestamp is kept, and does not move the window.
        check!(
            kept(stream(&["0", "nonsense", "5"]), Some(10)) == vec!["0", "nonsense"],
            "the odd entry is kept and 5 is still inside the window"
        );

        // A stream thinned to nothing cannot happen -- the first entry always
        // survives -- but a stream that was already empty is dropped.
        let mut empty = serde_json::json!({
            "data": {"result": [{"stream": {}, "values": []}]}
        });
        super::apply_loki_stream_interval(&mut empty, Some(10));
        check!(
            empty["data"]["result"]
                .as_array()
                .expect("an array")
                .is_empty(),
            "an empty stream is dropped rather than sent"
        );
    }

    /// `parse_prometheus_duration` is the value-computing sibling of
    /// `is_prometheus_duration_literal`: same grammar, but it sums the chunks.
    /// The units must still run from larger to smaller, and a repeat is
    /// refused by that same rule.
    ///
    /// The sum is checked against a duration using several units at once,
    /// since a single-unit value cannot show a chunk being dropped or
    /// multiplied by the wrong scale.
    #[test]
    fn a_prometheus_duration_sums_its_chunks_in_nanoseconds() {
        let parse = super::parse_prometheus_duration;
        let secs = 1_000_000_000_i64;

        // Each unit's own scale.
        check!(parse("1ns") == Some(1));
        check!(parse("1us") == Some(1_000));
        check!(parse("1ms") == Some(1_000_000));
        check!(parse("1s") == Some(secs));
        check!(parse("1m") == Some(60 * secs));
        check!(parse("1h") == Some(3_600 * secs));
        check!(parse("1d") == Some(24 * 3_600 * secs));
        check!(parse("1w") == Some(7 * 24 * 3_600 * secs));
        check!(parse("1y") == Some(365 * 24 * 3_600 * secs));

        // Several units summed, so a dropped chunk changes the total.
        check!(parse("1h30m") == Some(5_400 * secs));
        check!(parse("1h1m1s") == Some(3_661 * secs));
        check!(parse("2h2m2s") == Some(7_322 * secs));
        check!(parse("1s500ms") == Some(1_500_000_000));

        // Counts above one, and zero.
        check!(parse("90s") == Some(90 * secs));
        check!(parse("0s") == Some(0));
        check!(parse("0h0m0s") == Some(0));

        // The same refusals as the literal validator.
        check!(parse("30m1h").is_none(), "out of order");
        check!(parse("1h1h").is_none(), "repeated unit");
        check!(parse("1ms1m").is_none(), "ms is the smaller unit");
        check!(parse("").is_none());
        check!(parse("1").is_none(), "no unit");
        check!(parse("h").is_none(), "no count");
        check!(parse("1x").is_none(), "unknown unit");
        check!(parse("1.5h").is_none(), "not an integer count");

        // A total that will not fit is refused rather than wrapping.
        check!(
            parse("999999999999y").is_none(),
            "overflow is not a duration"
        );
    }

    /// `hex_string` renders bytes as lower-case hex, high nibble first. The
    /// byte 0xAB is the case that matters: with a symmetric byte like 0xAA a
    /// swapped nibble order is invisible.
    #[test]
    fn hex_rendering_puts_the_high_nibble_first() {
        let hex = super::hex_string;

        check!(hex(&[0xAB]) == "ab", "high nibble first");
        check!(hex(&[0x0F]) == "0f", "a leading zero is kept");
        check!(hex(&[0xF0]) == "f0");
        check!(hex(&[0x00]) == "00");
        check!(hex(&[0xFF]) == "ff");
        check!(hex(&[0x01, 0x23]) == "0123", "bytes in order");
        check!(hex(&[]) == "");
        check!(hex(&[0xDE, 0xAD, 0xBE, 0xEF]) == "deadbeef");
    }

    /// The two error classifiers decide whether a compaction failure came from
    /// the OBJECT STORE, which is the retryable kind -- a transient 503 should
    /// be retried where a malformed block never will be. Misclassifying either
    /// way is bad in its own direction: retrying a permanent failure spins,
    /// and giving up on a transient one loses data.
    #[test]
    fn only_an_object_store_failure_is_classified_as_retryable() {
        use krabka_blockstore::LogBlockStoreError as BlockStoreError;

        let is_object_store = super::compaction_error_is_object_store;
        let object_store_error = || {
            BlockStoreError::ObjectStore(object_store::Error::NotFound {
                path: "block".to_string(),
                source: "gone".into(),
            })
        };

        // The one that is.
        check!(super::block_store_error_is_object_store(
            &object_store_error()
        ));
        check!(is_object_store(&super::CompactionError::BlockStore(
            object_store_error()
        )));

        // Every other block-store failure is not, including an I/O error,
        // which also arrives while talking to storage but is not the object
        // store reporting it.
        let others = || {
            vec![
                BlockStoreError::EmptyBlockScan,
                BlockStoreError::InvalidTimeRange {
                    start_ns: 10,
                    end_ns: 1,
                },
                BlockStoreError::InvalidManifestVersion {
                    actual: 1,
                    expected: 2,
                },
                BlockStoreError::Io(std::io::Error::from(std::io::ErrorKind::NotFound)),
            ]
        };
        for error in others() {
            check!(!super::block_store_error_is_object_store(&error), "{error}");
        }
        for error in others() {
            check!(!is_object_store(&super::CompactionError::BlockStore(error)));
        }

        // And every compaction failure that is not a block-store one at all.
        check!(!is_object_store(&super::CompactionError::EmptyWalBatch));
        check!(!is_object_store(&super::CompactionError::AllRowsDeleted));
        check!(!is_object_store(
            &super::CompactionError::MissingWalPosition { timestamp_ns: 1 }
        ));
        check!(!is_object_store(&super::CompactionError::MixedTenant {
            expected: "a".to_string(),
            actual: "b".to_string(),
        }));
        check!(!is_object_store(&super::CompactionError::MixedPartition {
            expected: 1,
            actual: 2,
        }));
    }

    /// `prometheus_alert_key_matches_rule` picks out the alerts belonging to
    /// one rule that were NOT seen in this evaluation -- the ones that may need
    /// retaining as resolved. All four conditions are and-ed, so each is broken
    /// on its own against a key the other three accept.
    ///
    /// The last is the negated one: a key still active this round is excluded,
    /// which is what stops a firing alert being retained twice.
    #[test]
    fn a_retained_alert_key_belongs_to_its_rule_and_was_not_just_seen() {
        let key = |tenant: &str, alert: &str, query: &str| super::PrometheusAlertKey {
            tenant: tenant.to_string(),
            alert_name: alert.to_string(),
            query: query.to_string(),
            labels: Labels::default(),
        };
        let subject = key("tenant", "HighErrors", "up");
        let active = BTreeSet::new();
        let templates = Labels::default();
        let params = |active_keys| super::PrometheusRetainedAlertParams {
            tenant: "tenant",
            alert_name: "HighErrors",
            query: "up",
            evaluation_time: 0,
            hold_duration_ns: 0,
            keep_firing_for_ns: 0,
            active_keys,
            annotation_templates: &templates,
        };

        check!(super::prometheus_alert_key_matches_rule(
            &subject,
            &params(&active)
        ));

        // Each of the three identity fields, wrong on its own.
        check!(!super::prometheus_alert_key_matches_rule(
            &key("other", "HighErrors", "up"),
            &params(&active)
        ));
        check!(!super::prometheus_alert_key_matches_rule(
            &key("tenant", "Other", "up"),
            &params(&active)
        ));
        check!(!super::prometheus_alert_key_matches_rule(
            &key("tenant", "HighErrors", "down"),
            &params(&active)
        ));

        // And the negated one: a key seen this round is not retained.
        let mut seen = BTreeSet::new();
        seen.insert(subject.clone());
        check!(
            !super::prometheus_alert_key_matches_rule(&subject, &params(&seen)),
            "an alert still firing is not also retained"
        );
        // A different key being active does not exclude this one.
        let mut other_seen = BTreeSet::new();
        other_seen.insert(key("tenant", "HighErrors", "other"));
        check!(super::prometheus_alert_key_matches_rule(
            &subject,
            &params(&other_seen)
        ));
    }

    /// `matches_rule` filters the rules response by kind, by name, and by label
    /// selector. The three are independent AND conditions, each inactive when
    /// its filter is unset, so each is broken on its own against a rule the
    /// other two accept.
    ///
    /// The label selectors nest differently from the rest: SELECTORS are
    /// or-ed and the matchers WITHIN a selector are and-ed, which is Loki's
    /// `match[]` semantics. A single selector with a single matcher cannot
    /// show that, so both nestings are exercised.
    #[test]
    fn a_rule_matches_only_when_every_active_filter_accepts_it() {
        use krabka_logql::{LabelMatcher, MatchOp, StreamQuery};

        let rule = serde_json::json!({"type": "alerting", "name": "HighErrors"});
        let source: serde_yaml::Value =
            serde_yaml::from_str("labels:\n  severity: page\n  team: infra\n")
                .expect("the source rule parses");
        let matcher = |name: &str, value: &str| LabelMatcher {
            name: name.to_string(),
            op: MatchOp::Equal,
            value: value.to_string(),
        };
        let selector = |matchers: Vec<LabelMatcher>| StreamQuery {
            matchers,
            pipeline: Vec::new(),
        };
        let filters =
            |kind, names: &[&str], selectors: Vec<StreamQuery>| super::PrometheusRulesFilters {
                rule_kind: kind,
                rule_names: names.iter().map(|name| (*name).to_string()).collect(),
                label_selectors: selectors,
                ..super::PrometheusRulesFilters::default()
            };

        // No filters at all accepts everything.
        check!(filters(None, &[], Vec::new()).matches_rule(&rule, &source));

        // Each filter alone, accepting and rejecting.
        check!(filters(Some("alerting"), &[], Vec::new()).matches_rule(&rule, &source));
        check!(!filters(Some("recording"), &[], Vec::new()).matches_rule(&rule, &source));
        check!(filters(None, &["HighErrors"], Vec::new()).matches_rule(&rule, &source));
        check!(!filters(None, &["Other"], Vec::new()).matches_rule(&rule, &source));
        check!(
            filters(None, &["Other", "HighErrors"], Vec::new()).matches_rule(&rule, &source),
            "any of the named rules"
        );

        // Matchers WITHIN a selector are and-ed: one wrong matcher rejects.
        check!(
            filters(None, &[], vec![selector(vec![matcher("severity", "page")])])
                .matches_rule(&rule, &source)
        );
        check!(
            !filters(
                None,
                &[],
                vec![selector(vec![
                    matcher("severity", "page"),
                    matcher("team", "billing"),
                ])],
            )
            .matches_rule(&rule, &source),
            "every matcher in a selector must match"
        );

        // SELECTORS are or-ed: one that fails does not reject if another
        // succeeds.
        check!(
            filters(
                None,
                &[],
                vec![
                    selector(vec![matcher("team", "billing")]),
                    selector(vec![matcher("team", "infra")]),
                ],
            )
            .matches_rule(&rule, &source),
            "any selector may match"
        );

        // All three active together, with one of them failing.
        check!(
            !filters(
                Some("alerting"),
                &["HighErrors"],
                vec![selector(vec![matcher("team", "billing")])],
            )
            .matches_rule(&rule, &source),
            "the label selector still rejects"
        );
    }

    /// `parse_patterns_params` requires query, start and end, defaults only the
    /// step, and takes the LAST value of a repeated parameter. That last part
    /// is the opposite of `parse_series_params`, which keeps the first -- the
    /// two live in the same file and differ only by an `is_none()` guard, so
    /// each is pinned with the contrast stated.
    ///
    /// Each required parameter names ITSELF when missing, so a client sending
    /// two of the three is told which one it forgot rather than a generic
    /// failure.
    #[test]
    fn patterns_params_require_three_and_take_the_last_of_each() {
        let parse = |query: &str| super::parse_patterns_params(Some(query));

        let params = parse("query=up&start=100&end=200").expect("all three present");
        check!(params.query == "up");
        check!(params.start == 100);
        check!(params.end == 200);
        check!(
            params.step == 1_000_000_000,
            "the step defaults to a second"
        );

        // A repeated parameter keeps the LAST value.
        let params = parse("query=a&query=b&start=100&end=200").expect("parses");
        check!(params.query == "b", "the last query, unlike series params");
        let params = parse("query=up&start=100&start=300&end=200").expect("parses");
        check!(params.start == 300, "and the last start");

        // An explicit step overrides the default.
        let params = parse("query=up&start=100&end=200&step=5s").expect("parses");
        check!(params.step == 5_000_000_000);

        // Each required parameter names itself when absent.
        check!(matches!(
            parse("start=100&end=200"),
            Err(HttpQueryError::MissingQueryParameter("query"))
        ));
        check!(matches!(
            parse("query=up&end=200"),
            Err(HttpQueryError::MissingQueryParameter("start"))
        ));
        check!(matches!(
            parse("query=up&start=100"),
            Err(HttpQueryError::MissingQueryParameter("end"))
        ));
        check!(matches!(
            super::parse_patterns_params(None),
            Err(HttpQueryError::MissingQueryParameter("query"))
        ));

        // A malformed bound is refused rather than dropped.
        check!(parse("query=up&start=nonsense&end=200").is_err());
    }

    /// `parse_vector_matching_modifier` reads an `on(...)`/`ignoring(...)`
    /// clause and returns BOTH the rendered modifier and the position just
    /// past it. The position is what the caller resumes from, so an
    /// off-by-one there leaves a stray bracket in the rest of the query --
    /// each case checks the remainder, not just the modifier.
    #[test]
    fn a_vector_matching_modifier_reports_where_it_ended() {
        let parse = super::parse_vector_matching_modifier;
        let after = |query: &str, position: usize| {
            parse(query, position).map(|(modifier, end)| (modifier, query[end..].to_string()))
        };

        check!(
            after("on(job) foo", 0) == Some(("on (job)".to_string(), " foo".to_string())),
            "the remainder starts after the closing bracket"
        );
        check!(
            after("ignoring(pod) foo", 0)
                == Some(("ignoring (pod)".to_string(), " foo".to_string()))
        );
        check!(after("on(a,b) foo", 0) == Some(("on (a,b)".to_string(), " foo".to_string())));
        check!(
            after("on() foo", 0) == Some(("on ()".to_string(), " foo".to_string())),
            "an empty label list is still a modifier"
        );

        // Parsing from part-way in, which is how the caller uses it.
        check!(
            after("up on(job) foo", 3) == Some(("on (job)".to_string(), " foo".to_string())),
            "the position is an offset into the whole query"
        );

        // Not a modifier at this position.
        check!(parse("foo on(job)", 0).is_none());
        check!(parse("", 0).is_none());
        // The bracket must follow immediately: a space between is not this
        // spelling, and neither is an unclosed list.
        check!(parse("on (job)", 0).is_none());
        check!(parse("on(job", 0).is_none());
    }

    /// `format_logfmt_parser_flags` renders a parser's options back into the
    /// query text. The leading space belongs to the FLAGS, not to the caller:
    /// with no flags the string is empty rather than a lone space, which would
    /// otherwise leave a trailing space in every query without options.
    #[test]
    fn logfmt_parser_flags_carry_their_own_leading_space() {
        use krabka_logql::{LogfmtExtraction, LogfmtParserConfig};

        // The flags are only accepted alongside an extraction, so every
        // config here names one.
        let flags = |strict, keep_empty| {
            let extraction = LogfmtExtraction::same("level").expect("a valid extraction");
            let config = LogfmtParserConfig::with_options(vec![extraction], strict, keep_empty)
                .expect("the options are valid");
            super::format_logfmt_parser_flags(&config)
        };

        check!(flags(false, false) == "", "no flags, no space");
        check!(flags(true, false) == " --strict");
        check!(flags(false, true) == " --keep-empty");
        check!(
            flags(true, true) == " --keep-empty --strict",
            "both, in a fixed order, sharing one leading space"
        );
    }

    /// `log_pattern_token` masks the variable part of a log token so lines that
    /// differ only in their ids collapse to one pattern. A `key=value` token
    /// keeps its KEY and masks only the value, because the key is what makes
    /// two lines the same kind of line.
    #[test]
    fn a_log_pattern_token_masks_only_its_variable_part() {
        let token = super::log_pattern_token;

        // A bare token is masked whole, or kept whole.
        check!(token("connected") == "connected", "a word is not variable");
        check!(token("12345") == "<_>", "a number is");
        check!(token("1.5") == "<_>");

        // A key=value token keeps the key and masks the value.
        check!(token("user_id=12345") == "user_id=<_>");
        check!(
            token("status=ok") == "status=ok",
            "a non-variable value is kept"
        );
        check!(
            token("id=550e8400-e29b-41d4-a716-446655440000") == "id=<_>",
            "a uuid is variable"
        );

        // Half a pair is not a pair: an empty key or value leaves the token
        // alone rather than producing "=<_>" or "<_>=".
        check!(token("=12345") == "=12345");
        check!(token("user_id=") == "user_id=");
        check!(token("=") == "=");

        // Only the FIRST equals splits, so a value containing one is masked
        // whole rather than re-split.
        check!(
            token("q=a=12345") == "q=a=12345",
            "the value is not variable"
        );
        check!(token("") == "");
    }

    /// `could_be_scalar_vector_expression` is the cheap gate two of the query
    /// parsers run before doing real work. It admits anything starting like a
    /// number or a parenthesis, and among identifiers ONLY the three functions
    /// that can produce a vector -- so `sum(...)` is turned away here and
    /// parsed elsewhere.
    #[test]
    fn only_a_number_or_a_vector_function_could_be_a_scalar_vector_expression() {
        let could_be = super::could_be_scalar_vector_expression;

        // Numbers and the characters a numeric expression can open with.
        check!(could_be("1"));
        check!(could_be("1+1"));
        check!(could_be("+1"));
        check!(could_be("-1"));
        check!(could_be(".5"));
        check!(could_be("(1+1)"));
        check!(could_be("  1"), "leading whitespace is trimmed");

        // The three vector-producing functions, and nothing else.
        check!(could_be("vector(1)"));
        check!(could_be("label_replace(vector(1),\"a\",\"b\",\"c\",\"d\")"));
        check!(could_be("label_join(vector(1),\"a\",\"b\")"));
        check!(
            !could_be("sum(rate(x[5m]))"),
            "an aggregation is parsed elsewhere"
        );
        check!(!could_be("up"));

        // The identifier must match WHOLE: a longer name starting with one of
        // the three is not one of them.
        check!(!could_be("vectorise(1)"));
        check!(!could_be("vector_total"));

        // Nothing, and things that start with neither.
        check!(!could_be(""));
        check!(!could_be("   "));
        check!(
            !could_be("{app=\"a\"}"),
            "a matcher is not a scalar expression"
        );
        check!(!could_be("\"quoted\""));
    }

    /// `insert_descriptor_labels` copies a block's series labels from one index
    /// to another, and REFUSES when the source cannot supply them. A missing
    /// series is a corrupt index rather than an empty block, so carrying on
    /// would write a manifest whose blocks reference series nothing can name.
    #[test]
    fn copying_descriptor_labels_refuses_a_series_the_source_cannot_name() {
        use krabka_blockstore::{BlockDescriptor, BlockKey, LabelIndex, TimeRange};

        let mut source = LabelIndex::default();
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        let known = source.insert_series("tenant", labels.clone());
        let mut other = Labels::default();
        other.insert("app".to_string(), "web".to_string());
        let also_known = source.insert_series("tenant", other.clone());

        let descriptor = |fingerprints: &[_]| {
            BlockDescriptor::new(
                BlockKey::new("tenant", 0, 0, 1, TimeRange::new(0, 10).expect("a range")),
                fingerprints.iter().copied().collect(),
            )
        };

        // Both series are known, so both are copied.
        let mut target = LabelIndex::default();
        super::insert_descriptor_labels(
            &mut target,
            &source,
            "tenant",
            &descriptor(&[known, also_known]),
        )
        .expect("both series are known");
        check!(target.labels_for("tenant", known) == Some(&labels));
        check!(target.labels_for("tenant", also_known) == Some(&other));

        // A fingerprint the source has never seen is refused, and the error
        // names which one so the corruption can be found.
        let mut target = LabelIndex::default();
        let stranger = LabelIndex::default().insert_series("tenant", {
            let mut labels = Labels::default();
            labels.insert("app".to_string(), "stranger".to_string());
            labels
        });
        check!(matches!(
            super::insert_descriptor_labels(
                &mut target,
                &source,
                "tenant",
                &descriptor(&[stranger])
            ),
            Err(CompactorRunError::MissingSeriesLabels { .. })
        ));

        // The labels belong to a TENANT, so the right fingerprint under the
        // wrong tenant is just as unknown.
        let mut target = LabelIndex::default();
        check!(
            super::insert_descriptor_labels(&mut target, &source, "other", &descriptor(&[known]))
                .is_err(),
            "a fingerprint is not global"
        );

        // A descriptor with no series copies nothing and succeeds.
        let mut target = LabelIndex::default();
        super::insert_descriptor_labels(&mut target, &source, "tenant", &descriptor(&[]))
            .expect("an empty descriptor is not an error");
        check!(target.labels_for("tenant", known).is_none());
    }

    /// `read_loki_rule_tenants` treats a MISSING rules file as no rules, and
    /// every other I/O failure as an error. That distinction is the point: a
    /// store that has never had a rule written to it has no file, and starting
    /// up must not fail because of it -- while a file that exists and cannot
    /// be read is a real problem the operator needs told about.
    ///
    /// Malformed JSON is likewise an error rather than an empty result:
    /// silently discarding every rule in a corrupt file would stop alerting
    /// without saying so.
    #[test]
    fn missing_loki_rules_are_empty_but_unreadable_ones_are_an_error() {
        let dir = tempfile::tempdir().expect("a temp dir");

        // Absent: no rules, no error.
        let absent = dir.path().join("absent.json");
        let tenants =
            super::read_loki_rule_tenants(&absent).expect("an absent file is not an error");
        check!(tenants.is_empty());

        // Present and valid: the rules come back.
        let valid = dir.path().join("valid.json");
        std::fs::write(
            &valid,
            r#"{"tenant-a":{"namespace":{"group":{"rules":[]}}}}"#,
        )
        .expect("the fixture writes");
        let tenants = super::read_loki_rule_tenants(&valid).expect("valid json parses");
        check!(tenants.len() == 1);
        check!(tenants.contains_key("tenant-a"));

        // Present and empty-but-valid.
        let empty = dir.path().join("empty.json");
        std::fs::write(&empty, "{}").expect("the fixture writes");
        check!(
            super::read_loki_rule_tenants(&empty)
                .expect("an empty object parses")
                .is_empty()
        );

        // Present and malformed: an error, NOT an empty set. Returning empty
        // here would silently stop alerting on every rule in the file.
        let malformed = dir.path().join("malformed.json");
        std::fs::write(&malformed, "{not json").expect("the fixture writes");
        check!(matches!(
            super::read_loki_rule_tenants(&malformed),
            Err(super::LokiRuleStoreError::Json { .. })
        ));

        // A directory where a file was expected is an I/O error, which is how
        // a non-NotFound failure is reached without special privileges.
        check!(matches!(
            super::read_loki_rule_tenants(dir.path()),
            Err(super::LokiRuleStoreError::Io { .. })
        ));
    }

    /// `scalar_vector_expression_result` evaluates the scalar-and-vector
    /// sub-language: arithmetic over numbers, and `vector(...)` producing a
    /// series. Two things about it are easy to get wrong and are pinned here.
    ///
    /// First, whitespace is stripped BEFORE parsing rather than skipped during
    /// it, so "1 + 1" and "1+1" are the same expression -- and so, less
    /// happily, are "1 1" and "11". Second, the parser
    /// must be FINISHED: "1+1x" is refused rather than evaluated as "1+1" with
    /// the tail ignored, which would silently accept a typo as a valid query.
    #[test]
    fn a_scalar_vector_expression_must_consume_its_whole_query() {
        use super::ScalarVectorExpressionResult;

        let result = super::scalar_vector_expression_result;
        let scalar = |query: &str| match result(query) {
            Some(ScalarVectorExpressionResult::Scalar { sample }) => Some(sample),
            _ => None,
        };

        // Plain arithmetic, with and without spaces.
        check!(scalar("1").as_deref() == Some("1"));
        check!(scalar("1+1").as_deref() == Some("2"));
        check!(
            scalar("1 + 1").as_deref() == Some("2"),
            "whitespace is stripped first"
        );
        check!(scalar("  2 * 3  ").as_deref() == Some("6"));
        check!(
            scalar("(1+2)*3").as_deref() == Some("9"),
            "parentheses group"
        );

        // A vector literal is the other result shape.
        check!(matches!(
            result("vector(1)"),
            Some(ScalarVectorExpressionResult::Vector { .. })
        ));
        check!(
            matches!(
                result("vector( 1 )"),
                Some(ScalarVectorExpressionResult::Vector { .. }),
            ),
            "whitespace inside the call too"
        );

        // Trailing junk is refused rather than ignored. This is the case that
        // the `is_finished` check exists for: without it "1+1x" evaluates to 2
        // and a typo becomes a valid query.
        check!(result("1+1x").is_none());
        check!(result("vector(1)x").is_none());
        // But "1 1" is not junk -- stripping whitespace FIRST makes it the
        // single number eleven. That follows from the strip being a rewrite of
        // the input rather than a skip during parsing, and it is pinned
        // because it is surprising, not because it is desirable.
        check!(scalar("1 1").as_deref() == Some("11"));

        // A set operator needs a vector on BOTH sides. Each of the two counts
        // is a strict increase over the terms seen before that side was
        // parsed, and "at least as many" is trivially true -- so a side with
        // no vector at all is the only thing that separates them.
        check!(matches!(
            result("vector(1) and vector(2)"),
            Some(ScalarVectorExpressionResult::Vector { .. })
        ));
        check!(result("1 and vector(1)").is_none(), "no vector on the left");
        check!(result("vector(1) and 1").is_none(), "none on the right");
        check!(result("1 and 1").is_none(), "none on either side");

        // A comparison carrying `on(...)`/`ignoring(...)` needs a vector on
        // both sides too. Without a modifier the same comparison is fine, so
        // the modifier is what turns the requirement on.
        check!(
            result("vector(1) > 0").is_some(),
            "no modifier, no requirement"
        );
        check!(matches!(
            result("vector(1) > on() vector(2)"),
            Some(ScalarVectorExpressionResult::Vector { .. })
        ));
        check!(
            result("1 > on() vector(1)").is_none(),
            "a modifier with no vector on the left"
        );
        check!(
            result("vector(1) > ignoring() 1").is_none(),
            "and none on the right"
        );

        // An escape inside a string literal is decoded, and the parser advances
        // PAST it. Every other string here is escape-free, where advancing the
        // wrong way would go unnoticed.
        let replaced = result(r#"label_replace(vector(1),"dst","a\nb","src","(.*)")"#);
        let Some(ScalarVectorExpressionResult::Vector { metric, .. }) = replaced else {
            panic!("expected a vector result");
        };
        check!(
            metric.get("dst").map(String::as_str) == Some("a\nb"),
            "got {metric:?}"
        );

        // Not this sub-language at all.
        check!(result("up").is_none());
        check!(result("").is_none());
        check!(result("+").is_none());
        check!(result("(1").is_none(), "an unclosed group is not finished");
    }

    /// `format_loki_query_length` always writes all three units, including the
    /// zero ones -- "0h5m0s" rather than "5m". That is the opposite of
    /// `format_loki_duration_ns`, which skips empty units, and the two are
    /// pinned separately because the difference is deliberate: this one is a
    /// fixed-shape field a client parses positionally.
    #[test]
    fn a_loki_query_length_always_writes_all_three_units() {
        let format = |seconds: i64| super::format_loki_query_length(Time::from_nanos(seconds));
        let secs = 1_000_000_000_i64;

        check!(format(0) == "0h0m0s", "every unit, even at zero");
        check!(format(5 * secs) == "0h0m5s");
        check!(format(300 * secs) == "0h5m0s", "zero seconds still written");
        check!(format(3_600 * secs) == "1h0m0s");
        check!(format(3_661 * secs) == "1h1m1s");
        check!(format(7_322 * secs) == "2h2m2s");

        // Hours accumulate rather than rolling into a larger unit.
        check!(format(100 * 3_600 * secs) == "100h0m0s");

        // Sub-second precision is dropped, not rounded up.
        check!(format(secs - 1) == "0h0m0s");

        // A negative range is clamped to zero rather than writing minus signs
        // into a field a client parses positionally.
        check!(format(-secs) == "0h0m0s");
    }

    /// `validate_loki_interval` refuses a negative step and accepts everything
    /// else, including zero and an absent one. Zero is the boundary that
    /// separates `< 0` from `<= 0`, and an absent interval is not the same as
    /// a zero one -- absent means the caller did not ask.
    #[test]
    fn a_loki_interval_is_refused_only_when_negative() {
        let validate = super::validate_loki_interval;

        check!(validate(None).is_ok(), "an absent interval is not an error");
        check!(validate(Some(0)).is_ok(), "and neither is zero");
        check!(validate(Some(1)).is_ok());
        check!(validate(Some(i64::MAX)).is_ok());
        check!(matches!(
            validate(Some(-1)),
            Err(HttpQueryError::InvalidInterval)
        ));
        check!(validate(Some(i64::MIN)).is_err());
    }

    /// `normalize_loki_vector_sample_timestamps_to_seconds` rewrites each
    /// instant sample's timestamp from nanoseconds to seconds in place. It
    /// accepts the timestamp as a JSON number OR a string, since both spellings
    /// reach it, and it writes back a whole number when the nanos divide
    /// exactly and a float otherwise -- a client parsing "1700000000" as an
    /// integer must not be handed "1700000000.0".
    #[test]
    fn loki_vector_timestamps_are_rewritten_from_nanos_to_seconds() {
        let normalize = |timestamp: serde_json::Value| {
            let mut value = serde_json::json!({
                "data": {"result": [{"metric": {}, "value": [timestamp, "1"]}]}
            });
            super::normalize_loki_vector_sample_timestamps_to_seconds(&mut value);
            value["data"]["result"][0]["value"][0].clone()
        };

        // An exact second becomes an integer, in both spellings.
        check!(normalize(serde_json::json!(1_700_000_000_000_000_000_u64)) == 1_700_000_000);
        check!(normalize(serde_json::json!("1700000000000000000")) == 1_700_000_000);

        // A fractional second becomes a float rather than being truncated.
        check!(normalize(serde_json::json!(1_500_000_000_u64)) == 1.5);
        check!(normalize(serde_json::json!("1500000000")) == 1.5);

        check!(normalize(serde_json::json!(0_u64)) == 0);

        // A timestamp that is neither a number nor a string is left alone
        // rather than replaced with a default.
        check!(normalize(serde_json::json!(true)) == true);

        // A response with no result array is left untouched.
        let mut empty = serde_json::json!({"status": "success"});
        let before = empty.clone();
        super::normalize_loki_vector_sample_timestamps_to_seconds(&mut empty);
        check!(empty == before);
    }

    /// `parse_metric_vector_comparison_expression` recognises a comparison
    /// between a metric query and a `vector(...)` literal, and records WHICH
    /// side the literal was on -- the two are not interchangeable, since
    /// `up > vector(1)` and `vector(1) > up` select opposite samples.
    ///
    /// Exactly one side must be a vector literal. Two of them, or none, is not
    /// this kind of expression and is refused rather than guessed at, so both
    /// rejections are checked as well as both acceptances.
    #[test]
    fn a_vector_comparison_records_which_side_the_literal_was_on() {
        use krabka_logql::ComparisonOp;

        let parse = super::parse_metric_vector_comparison_expression;

        let right = parse("up > vector(1)").expect("a vector on the right");
        check!(right.metric_query == "up");
        check!(right.vector_query == "vector(1)");
        check!(!right.vector_on_left);
        check!(right.op == ComparisonOp::Greater);
        check!(!right.bool_modifier);

        let left = parse("vector(1) > up").expect("a vector on the left");
        check!(left.metric_query == "up", "the metric is still the metric");
        check!(left.vector_query == "vector(1)");
        check!(left.vector_on_left, "but the side is recorded");
        check!(left.op == ComparisonOp::Greater);

        // The `bool` modifier is stripped from the right and remembered.
        let modified = parse("up > bool vector(1)").expect("bool is allowed");
        check!(modified.bool_modifier);
        check!(
            modified.vector_query == "vector(1)",
            "bool is not part of the query"
        );
        check!(modified.metric_query == "up");

        // Every comparison operator reaches the expression.
        for (query, op) in [
            ("up == vector(1)", ComparisonOp::Equal),
            ("up != vector(1)", ComparisonOp::NotEqual),
            ("up < vector(1)", ComparisonOp::Less),
            ("up <= vector(1)", ComparisonOp::LessEqual),
            ("up >= vector(1)", ComparisonOp::GreaterEqual),
        ] {
            check!(parse(query).expect("parses").op == op, "{query}");
        }

        // Two vectors, or none, is not this kind of expression.
        check!(parse("vector(1) > vector(2)").is_none(), "two literals");
        check!(parse("up > down").is_none(), "no literal");
        check!(parse("up").is_none(), "no comparison at all");
        check!(parse("").is_none());
    }

    /// `request_query_or_form_body` picks ONE source rather than merging them,
    /// and the query string wins. That is the opposite of `log_level_post`,
    /// which merges and lets the body win -- the two are pinned separately
    /// because a reader who knows one would guess the other wrong.
    ///
    /// An empty source is not a source: an empty query string falls through to
    /// the body rather than being returned as an empty query, which would
    /// produce a "missing parameter" error naming the wrong cause.
    #[test]
    fn a_request_takes_its_query_from_the_string_before_the_body() {
        let take = |raw_query: Option<&str>, body: &[u8]| {
            super::request_query_or_form_body(raw_query, &axum::body::Bytes::from(body.to_vec()))
        };

        // The query string wins when both carry something.
        check!(take(Some("query=a"), b"query=b").ok().as_deref() == Some("query=a"));
        // Either alone.
        check!(take(Some("query=a"), b"").ok().as_deref() == Some("query=a"));
        check!(take(None, b"query=b").ok().as_deref() == Some("query=b"));

        // An empty query string is not a source, so the body is used.
        check!(take(Some(""), b"query=b").ok().as_deref() == Some("query=b"));

        // Neither source is a missing-parameter error, distinct from a
        // malformed one.
        check!(matches!(
            take(None, b""),
            Err(HttpQueryError::MissingQueryParameter("query"))
        ));
        check!(matches!(
            take(Some(""), b""),
            Err(HttpQueryError::MissingQueryParameter("query"))
        ));

        // A body that is not UTF-8 is refused rather than read lossily: a
        // replacement character in a matcher would change what was queried.
        check!(matches!(
            take(None, &[0xff, 0xfe]),
            Err(HttpQueryError::InvalidPercentEncoding)
        ));
        // But only when the body is the source being used.
        check!(
            take(Some("query=a"), &[0xff, 0xfe]).ok().as_deref() == Some("query=a"),
            "an unused body is not validated"
        );
    }

    /// `split_query_param_pairs` breaks a query string on `&` only when a
    /// KNOWN key follows it. That is not the usual rule, and it exists because
    /// a `LogQL` matcher can contain an ampersand -- splitting on every one
    /// would cut a query in half and leave both halves unparseable.
    #[test]
    fn a_query_string_splits_only_before_a_known_key() {
        fn split(query: &str) -> Vec<&str> {
            super::split_query_param_pairs(query, &["query", "start", "end"])
        }

        check!(split("query=up") == vec!["query=up"]);
        check!(split("query=up&start=1") == vec!["query=up", "start=1"]);
        check!(split("query=up&start=1&end=2") == vec!["query=up", "start=1", "end=2"]);

        // An ampersand inside a value is kept, because what follows it is not
        // a known key. This is the case the whole function exists for.
        check!(
            split(r#"query={app="a&b"}&start=1"#) == vec![r#"query={app="a&b"}"#, "start=1"],
            "the matcher keeps its ampersand"
        );
        check!(
            split("query=a&b=c") == vec!["query=a&b=c"],
            "b is not a known key"
        );

        // A known key needs its `=` to count as one: "&start" alone is text.
        check!(split("query=a&start") == vec!["query=a&start"]);
        check!(
            split("query=a&startle=1") == vec!["query=a&startle=1"],
            "not a prefix match"
        );

        // Empty segments are dropped rather than yielded as empty strings.
        check!(split("") == Vec::<&str>::new());
        check!(split("&query=a") == vec!["query=a"]);
        // A trailing `&` is KEPT, since nothing follows it to be a known key.
        // The rule is about what comes after the ampersand, not about the
        // ampersand itself.
        check!(split("query=a&") == vec!["query=a&"]);
    }

    /// `parse_series_params` treats its parameters asymmetrically, and the
    /// asymmetry is deliberate: matchers ACCUMULATE, because a series request
    /// may carry several, while the time bounds are FIRST-WINS, because a
    /// second one is a client mistake rather than an addition. A fixture
    /// sending each parameter once cannot tell the two rules apart.
    #[test]
    fn series_params_accumulate_matchers_but_keep_the_first_time_bound() {
        let parse = |query: &str| super::parse_series_params(Some(query));

        // Both spellings of a matcher, accumulating in the order sent.
        let params = parse("match[]=a&match[]=b").expect("matchers parse");
        check!(params.matchers == vec!["a".to_string(), "b".to_string()]);
        let params = parse("query=a&query=b").expect("matchers parse");
        check!(params.matchers == vec!["a".to_string(), "b".to_string()]);
        // And the two spellings share one list.
        let params = parse("match[]=a&query=b").expect("matchers parse");
        check!(params.matchers == vec!["a".to_string(), "b".to_string()]);

        // The percent-encoded spelling of `match[]` is accepted too.
        let params = parse("match%5B%5D=a").expect("matchers parse");
        check!(params.matchers == vec!["a".to_string()]);

        // Time bounds keep the FIRST value, not the last. A bare integer is
        // read as nanoseconds directly rather than as seconds.
        let params = parse("start=100&start=200").expect("bounds parse");
        check!(params.start == Some(100), "the first bound, in nanoseconds");
        let params = parse("end=100&end=200").expect("bounds parse");
        check!(params.end == Some(100));
        // A decimal is seconds, and RFC3339 is accepted too -- three
        // spellings reaching one field.
        check!(parse("start=1.5").expect("decimal seconds").start == Some(1_500_000_000));
        check!(parse("start=1970-01-01T00:00:01Z").expect("rfc3339").start == Some(1_000_000_000));

        // Absent parameters stay absent rather than defaulting.
        let params = parse("query=a").expect("a query alone parses");
        check!(params.start.is_none());
        check!(params.end.is_none());
        check!(params.since.is_none());

        // No query string at all is not an error.
        let params = super::parse_series_params(None).expect("no query is valid");
        check!(params.matchers.is_empty());

        // Unknown parameters are ignored rather than refused.
        check!(
            parse("nonsense=1")
                .expect("unknown keys are ignored")
                .matchers
                .is_empty()
        );

        // A malformed bound IS refused, since silently dropping it would run
        // the query over a window the client did not ask for.
        check!(parse("start=nonsense").is_err());
    }

    /// `format_loki_duration_ns` composes a duration from the largest unit
    /// down, SKIPPING units that contribute nothing -- so 3661s is "1h1m1s"
    /// and not "1h1m1s0ms0us0ns". Zero is the one duration spelled with a unit
    /// it does not contain, because "" would not read as a duration at all.
    #[test]
    fn a_loki_duration_composes_only_the_units_it_needs() {
        let format = super::format_loki_duration_ns;

        // Each unit alone.
        check!(format(3_600_000_000_000) == Some("1h".to_string()));
        check!(format(60_000_000_000) == Some("1m".to_string()));
        check!(format(1_000_000_000) == Some("1s".to_string()));
        check!(format(1_000_000) == Some("1ms".to_string()));
        check!(format(1_000) == Some("1us".to_string()));
        check!(format(1) == Some("1ns".to_string()));

        // Composed, with the gaps left out rather than written as zeros.
        check!(format(3_661_000_000_000) == Some("1h1m1s".to_string()));
        check!(
            format(3_600_000_000_001) == Some("1h1ns".to_string()),
            "no zero units between"
        );
        check!(format(90_000_000_000) == Some("1m30s".to_string()));
        check!(format(1_500_000) == Some("1ms500us".to_string()));

        // Counts above one, and a unit that repeats rather than rolling over
        // into the next -- 90 minutes is an hour and a half, not "90m".
        check!(format(2 * 3_600_000_000_000) == Some("2h".to_string()));
        check!(format(90 * 60_000_000_000) == Some("1h30m".to_string()));

        // Zero and negative are different answers: a zero duration is a
        // duration, a negative one is not.
        check!(format(0) == Some("0s".to_string()));
        check!(format(-1).is_none());
        check!(format(-3_600_000_000_000).is_none());
    }

    /// `is_bytes_literal` accepts "1MB" and "1.5GiB": a non-negative finite
    /// number followed by a unit it knows. The split is at the first letter,
    /// so the number and the unit are never ambiguous -- and both the decimal
    /// and binary spellings of each magnitude are units, since Loki accepts
    /// both.
    #[test]
    fn a_bytes_literal_needs_a_number_and_a_unit_it_knows() {
        let is_bytes = super::is_bytes_literal;

        for unit in [
            "B", "kB", "KB", "MB", "GB", "TB", "KiB", "MiB", "GiB", "TiB",
        ] {
            check!(is_bytes(&format!("1{unit}")), "{unit}");
        }
        check!(is_bytes("1.5GiB"), "a fractional amount");
        check!(is_bytes("0B"), "zero bytes is a size");

        // A number with no unit, or a unit with no number.
        check!(!is_bytes("1"));
        check!(!is_bytes(""));
        check!(!is_bytes("MB"), "the amount is empty, which does not parse");

        // Units it does not know, including near-misses.
        check!(!is_bytes("1PB"));
        check!(!is_bytes("1mb"), "the units are case-sensitive");
        check!(!is_bytes("1MBs"));
        check!(!is_bytes("1Mib"));

        // A negative amount is refused rather than clamped to zero.
        check!(!is_bytes("-1MB"));

        // "inf" and "NaN" contain letters, so the split puts them in the UNIT
        // and leaves the amount empty -- they are refused for having no
        // number, not for being non-finite.
        check!(!is_bytes("infMB"));
        check!(!is_bytes("NaNMB"));

        // The finiteness check is reached by a number with no letters in it at
        // all: four hundred digits overflow an f64 to infinity, and a size of
        // infinity is not a size.
        let overflowing = format!("{}MB", "1".repeat(400));
        check!(
            !is_bytes(&overflowing),
            "an amount that overflows to infinity"
        );
    }

    /// `eligible_tail_record_count` holds a tail back by `delay_for`, so a
    /// consumer sees only records old enough that nothing earlier can still
    /// arrive. It counts with `take_while` rather than `filter`: the WAL is
    /// ordered, so the first record too new to send BLOCKS the ones after it
    /// even if those happen to be older. Sending past it would emit records
    /// out of order, which is worse than sending them late.
    #[test]
    fn a_tail_holds_back_records_newer_than_its_delay() {
        let record = |timestamp_ns| super::WalLogRecord {
            tenant: "tenant".to_string(),
            labels: Labels::default(),
            timestamp_ns,
            line: "line".to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        };
        let count = super::eligible_tail_record_count;
        // Comfortably either side of now, so the wall clock cannot straddle
        // them however long the test takes to reach this line.
        let old = 1_000_000_000_000_i64;
        let future = i64::MAX / 2;

        // No delay means no holding back, whatever the timestamps.
        check!(count(&[record(old), record(future)], 0) == 2);
        check!(
            count(&[record(future)], -1) == 1,
            "a negative delay is not a delay"
        );

        // With a delay, old records are eligible and future ones are not.
        check!(count(&[record(old), record(old)], 1) == 2);
        check!(count(&[record(future)], 1) == 0);

        // The cutoff is `now - delay`, and only a record BETWEEN the two
        // possible cutoffs shows that: with an hour's delay a record stamped
        // now is held back, where `now + delay` would have released it. A
        // one-nanosecond delay moves the cutoff too little to tell.
        let hour_ns = 3_600 * 1_000_000_000_i64;
        check!(
            count(&[record(super::current_unix_time_ns())], hour_ns) == 0,
            "a record stamped now is newer than an hour ago"
        );

        // The first ineligible record stops the count: the second record here
        // is old enough on its own, and is still held back.
        check!(
            count(&[record(future), record(old)], 1) == 0,
            "take_while, not filter"
        );
        check!(count(&[record(old), record(future), record(old)], 1) == 1);

        check!(count(&[], 1) == 0);
        check!(count(&[], 0) == 0);
    }

    /// `apply_loki_tail_frame_limit` spends one budget across a frame's
    /// streams, and `tail_frame_is_empty` decides whether the result is worth
    /// sending at all. The two work together: the limiter drops streams it
    /// empties, so a frame limited down to nothing has no streams left and
    /// the emptiness check -- which reads the streams array, not the values
    /// inside it -- then suppresses the frame.
    #[test]
    fn a_tail_frame_limit_is_spent_across_streams_in_order() {
        let frame = |counts: &[usize]| {
            serde_json::json!({
                "streams": counts
                    .iter()
                    .map(|count| serde_json::json!({
                        "stream": {"app": "api"},
                        "values": (0..*count)
                            .map(|i| serde_json::json!([i.to_string(), "line"]))
                            .collect::<Vec<_>>(),
                    }))
                    .collect::<Vec<_>>(),
            })
        };
        let kept = |value: &serde_json::Value| {
            value["streams"]
                .as_array()
                .expect("streams is an array")
                .iter()
                .map(|stream| stream["values"].as_array().map_or(0, Vec::len))
                .collect::<Vec<_>>()
        };

        // The first stream takes 2 of the 5 and the second takes the rest.
        check!(
            kept(&super::apply_loki_tail_frame_limit(
                frame(&[2, 10]),
                Some(5)
            )) == vec![2, 3]
        );
        // A stream that exhausts the budget leaves nothing for the later ones,
        // and emptied streams are dropped rather than sent with no values --
        // the same rule as the search path.
        check!(
            kept(&super::apply_loki_tail_frame_limit(
                frame(&[5, 10]),
                Some(5)
            )) == vec![5]
        );
        check!(kept(&super::apply_loki_tail_frame_limit(frame(&[2, 2]), Some(5))) == vec![2, 2]);
        check!(kept(&super::apply_loki_tail_frame_limit(frame(&[9]), None)) == vec![9]);
        check!(
            kept(&super::apply_loki_tail_frame_limit(frame(&[9]), Some(0))).is_empty(),
            "a zero limit empties every stream, and empty streams are dropped"
        );

        // Emptiness is about the streams array, not the values in it.
        check!(super::tail_frame_is_empty(&frame(&[])));
        check!(super::tail_frame_is_empty(&serde_json::json!({})));
        check!(
            !super::tail_frame_is_empty(&frame(&[0])),
            "a stream carrying no values is still a stream"
        );
        check!(!super::tail_frame_is_empty(&frame(&[1])));
    }

    /// `consume_hot_metric_sample` spends one unit of a per-series, per-instant
    /// budget, and reports whether it could. Its three refusals are distinct
    /// causes -- the sample has no timestamp, the series and instant were never
    /// counted, or their budget is already spent -- and all three return the
    /// same false, so each is reached separately here.
    ///
    /// The decrement is the point: consuming twice from a budget of one must
    /// succeed then fail. A test that consumed once could not tell a decrement
    /// from a mere presence check.
    #[test]
    fn consuming_a_hot_metric_sample_spends_its_budget_once_per_unit() {
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        let other = Labels::default();
        let sample = serde_json::json!([1_700_000_000, "1"]);
        let key = |labels: &Labels| (labels.clone(), "1700000000".to_string());

        let mut counts = BTreeMap::new();
        counts.insert(key(&labels), 2_u64);

        // Two units budgeted, so two succeed and the third does not.
        check!(super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &sample
        ));
        check!(super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &sample
        ));
        check!(
            !super::consume_hot_metric_sample(&mut counts, &labels, &sample),
            "the budget is spent, not merely present"
        );
        check!(counts[&key(&labels)] == 0, "and it stops at zero");

        // A different series has its own budget, not this one's.
        check!(
            !super::consume_hot_metric_sample(&mut counts, &other, &sample),
            "an uncounted series has nothing to spend"
        );

        // A different instant of the SAME series likewise: the key is the pair.
        let later = serde_json::json!([1_700_000_001, "1"]);
        check!(!super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &later
        ));

        // A sample with no timestamp at all.
        check!(!super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &serde_json::json!([])
        ));
        check!(!super::consume_hot_metric_sample(
            &mut counts,
            &labels,
            &serde_json::json!("bare")
        ));
    }

    /// `loki_vector_sample_value` reads the VALUE half of an instant sample --
    /// index one, not zero -- and parses it. The timestamp beside it is also a
    /// number, so reading the wrong index yields something that parses fine and
    /// is simply wrong.
    #[test]
    fn a_loki_vector_sample_reads_its_value_and_not_its_timestamp() {
        let value = |sample: serde_json::Value| super::loki_vector_sample_value(&sample);
        let instant = |timestamp, sample_value| serde_json::json!({"metric": {}, "value": [timestamp, sample_value]});

        check!(value(instant(1_700_000_000_i64, "42")) == Some(MetricValue::new(42, 1)));
        check!(value(instant(1_700_000_000_i64, "1.5")) == Some(MetricValue::new(15, 10)));

        // The value is a STRING in Loki's encoding; a bare number is not read.
        check!(value(serde_json::json!({"value": [1, 42]})).is_none());
        // And an unparseable one is refused rather than defaulted to zero.
        check!(value(instant(1, "nonsense")).is_none());

        // Missing pieces: no value key, too short an array, not an array.
        check!(value(serde_json::json!({"metric": {}})).is_none());
        check!(value(serde_json::json!({"value": [1]})).is_none());
        check!(value(serde_json::json!({"value": "1"})).is_none());
    }

    /// `is_prometheus_duration_literal` accepts "1h30m" and refuses "30m1h":
    /// the units must run strictly from larger to smaller, which is what makes
    /// a duration unambiguous without needing to add the parts up. A repeat is
    /// refused by the same rule, since a unit is never strictly larger than
    /// itself -- which is why the ordering test is `<=` and not `<`.
    #[test]
    fn a_prometheus_duration_literal_runs_from_larger_units_to_smaller() {
        let is_duration = super::is_prometheus_duration_literal;

        // Every unit, in the one order that is allowed.
        check!(is_duration("1y2w3d4h5m6s7ms8us9ns"));
        for unit in ["y", "w", "d", "h", "m", "s", "ms", "us", "ns"] {
            check!(is_duration(&format!("1{unit}")), "{unit} alone");
        }
        check!(is_duration("1h30m"));
        check!(is_duration("90s"));
        check!(is_duration("0s"), "zero is a duration");

        // Out of order, in both the obvious and the subtle spelling. "1ms1m"
        // is the subtle one: read as text it looks ascending, but ms is the
        // SMALLER unit and so may not come first.
        check!(!is_duration("30m1h"));
        check!(!is_duration("1s1h"));
        check!(!is_duration("1ms1m"));
        check!(is_duration("1m1ms"), "that pair the right way round");
        check!(is_duration("1s1ms"), "and seconds before milliseconds");

        // A repeated unit, adjacent or separated.
        check!(!is_duration("1h1h"));
        check!(!is_duration("1h1m1h"));

        // Every chunk needs both a count and a unit.
        check!(!is_duration(""), "nothing is not a duration");
        check!(!is_duration("1"), "a bare number has no unit");
        check!(!is_duration("h"), "a bare unit has no count");
        check!(!is_duration("1h30"), "the trailing chunk has no unit");

        // Unknown units, and units that are only a prefix of a real one.
        check!(!is_duration("1x"));
        check!(!is_duration("1hh"));
        check!(!is_duration("1sec"));

        // Nothing else is allowed between chunks: no sign, no point, no space.
        check!(!is_duration("1.5h"));
        check!(!is_duration("-1h"));
        check!(!is_duration("1h "));
        check!(!is_duration("1h 30m"));
    }

    /// The in-place form of vector arithmetic: the left series is both the
    /// left operand and the output, so it is cloned before being written to.
    /// That clone is what keeps `a - b` from computing against a value it has
    /// already overwritten, and it only shows when the operator is
    /// non-commutative AND the result differs from the operand.
    #[test]
    fn in_place_vector_arithmetic_reads_the_left_operand_before_writing_it() {
        use krabka_logql::MetricScalarArithmeticOp;

        let series = |samples: &[(i64, &str)]| {
            serde_json::json!({
                "metric": {"app": "api"},
                "values": samples
                    .iter()
                    .map(|(ts, value)| serde_json::json!([ts, value]))
                    .collect::<Vec<_>>(),
            })
        };
        let pairs = |value: &serde_json::Value| {
            value
                .get("values")
                .and_then(serde_json::Value::as_array)
                .expect("the series has values")
                .iter()
                .map(|sample| {
                    (
                        sample[0].as_i64().expect("a timestamp"),
                        sample[1].as_str().expect("a value").to_string(),
                    )
                })
                .collect::<Vec<_>>()
        };
        // 2 and 3 have no right sample and are adjacent, so an index that
        // advanced on removal would keep one of them.
        let right = series(&[(1, "2"), (6, "1")]);
        let apply = |op| {
            let mut left = series(&[(1, "10"), (2, "20"), (3, "20"), (6, "7")]);
            let kept = super::apply_metric_binary_arithmetic_to_series(&mut left, &right, op);
            (kept, pairs(&left))
        };

        check!(
            apply(MetricScalarArithmeticOp::Subtract)
                == (true, vec![(1, "8".to_string()), (6, "6".to_string())]),
            "10-2 and 7-1, and the unmatched pair dropped"
        );
        check!(
            apply(MetricScalarArithmeticOp::Divide)
                == (true, vec![(1, "5".to_string()), (6, "7".to_string())])
        );

        // Everything dropped reports false so the caller can discard the
        // series rather than emit one with no samples.
        let mut orphan = series(&[(9, "1")]);
        check!(!super::apply_metric_binary_arithmetic_to_series(
            &mut orphan,
            &right,
            MetricScalarArithmeticOp::Subtract,
        ));

        // A right series with no values matches nothing at all.
        let mut left = series(&[(1, "10")]);
        check!(!super::apply_metric_binary_arithmetic_to_series(
            &mut left,
            &serde_json::json!({"metric": {}}),
            MetricScalarArithmeticOp::Subtract,
        ));

        // The instant shape, where the same clone-before-write applies to the
        // single sample.
        let instant =
            |ts: i64, value: &str| serde_json::json!({"metric": {}, "value": [ts, value]});
        let mut left = instant(1, "10");
        check!(super::apply_metric_binary_arithmetic_to_series(
            &mut left,
            &instant(1, "2"),
            MetricScalarArithmeticOp::Subtract,
        ));
        check!(left["value"][1] == "8", "10-2, not 2-10 and not 0");
    }

    /// `populate_loki_query_scan_stats` fills Loki's stats block, and the two
    /// per-source sections appear only when that source contributed. An empty
    /// `ingester` or `store` object would tell a client the source was
    /// consulted and returned nothing, which is a different claim from not
    /// having been consulted -- Grafana renders the two differently.
    ///
    /// The summary is unconditional and sums BOTH sources, so it is checked
    /// with each source alone as well as with both: with one contributing,
    /// a sum that dropped the other term still reads correctly.
    #[test]
    fn loki_scan_stats_report_only_the_sources_that_contributed() {
        let fill = |store_lines, ingester_lines, chunks| {
            let mut stats = serde_json::json!({});
            super::populate_loki_query_scan_stats(
                &mut stats,
                krabka_units::bytes(4_096),
                store_lines,
                ingester_lines,
                chunks,
            );
            stats
        };

        // Both sources contributed.
        let both = fill(7, 3, 2);
        check!(both["ingester"]["decompressedLines"] == 3);
        check!(both["ingester"]["totalLinesSent"] == 3);
        check!(both["store"]["decompressedLines"] == 7);
        check!(both["store"]["totalChunksRef"] == 2);
        check!(both["store"]["totalChunksDownloaded"] == 2);
        check!(both["store"]["compressedBytes"] == 4_096);
        check!(both["store"]["decompressedBytes"] == 4_096);
        check!(both["summary"]["totalBytesProcessed"] == 4_096);
        check!(
            both["summary"]["totalLinesProcessed"] == 10,
            "the summary sums store and ingester"
        );

        // Only the ingester: no store section at all, not an empty one.
        let hot = fill(0, 3, 0);
        check!(hot["ingester"]["decompressedLines"] == 3);
        check!(hot.get("store").is_none(), "absent, not empty");
        check!(hot["summary"]["totalLinesProcessed"] == 3);

        // Only the store: no ingester section.
        let cold = fill(7, 0, 2);
        check!(cold["store"]["decompressedLines"] == 7);
        check!(cold.get("ingester").is_none(), "absent, not empty");
        check!(cold["summary"]["totalLinesProcessed"] == 7);

        // Neither: the summary still reports, at zero.
        let empty = fill(0, 0, 0);
        check!(empty.get("store").is_none());
        check!(empty.get("ingester").is_none());
        check!(empty["summary"]["totalLinesProcessed"] == 0);
        check!(
            empty["summary"]["totalBytesProcessed"] == 4_096,
            "bytes are unconditional"
        );

        // The store section is gated on CHUNKS, not on lines: a chunk that
        // matched no lines was still downloaded and still cost bytes.
        let scanned_nothing = fill(0, 0, 2);
        check!(scanned_nothing["store"]["totalChunksRef"] == 2);
        check!(scanned_nothing["store"]["decompressedLines"] == 0);
    }

    /// `parse_decimal_seconds_timestamp` reads "seconds.fraction" as
    /// nanoseconds. It REQUIRES the point -- a bare integer is handled
    /// elsewhere, as seconds or as nanos depending on context, and guessing
    /// here would pre-empt that. The fraction is padded to nine places and
    /// truncated past them, so a microsecond timestamp scales correctly.
    ///
    /// The `take(9)` bounding that loop is belt-and-braces: the scale divides
    /// by ten each digit and reaches zero by integer division after the ninth,
    /// so a tenth digit contributes nothing whether it is read or not.
    /// Widening the take is an equivalent mutation.
    #[test]
    fn a_decimal_seconds_timestamp_scales_its_fraction_to_nanos() {
        let parse = super::parse_decimal_seconds_timestamp;

        // The fraction is positional: one digit is tenths, not nanos.
        check!(parse("5.5") == Some(5_500_000_000));
        check!(parse("5.05") == Some(5_050_000_000));
        check!(parse("0.000000001") == Some(1), "one nanosecond");
        check!(parse("1.000000000") == Some(1_000_000_000));

        // Past nine places the rest is dropped rather than rounded.
        check!(
            parse("0.0000000009") == Some(0),
            "a tenth of a nanosecond is lost"
        );
        check!(parse("1.9999999999") == Some(1_999_999_999));

        // Either side may be empty, but not both.
        check!(parse(".5") == Some(500_000_000));
        check!(parse("5.") == Some(5_000_000_000));
        check!(parse(".").is_none());

        // Signs, including a negative instant.
        check!(parse("-5.5") == Some(-5_500_000_000));
        check!(parse("+5.5") == Some(5_500_000_000));

        // The point is required: a bare integer is somebody else's problem.
        check!(parse("5").is_none(), "no point, no answer");
        check!(parse("").is_none());
        check!(parse("abc").is_none());
        check!(parse("5.abc").is_none());
        check!(parse("5.5.5").is_none(), "the second point is not a digit");
    }

    /// `metric_binary_sample_timestamp_ns_candidates` offers every reading a
    /// sample's timestamp could plausibly have. Which readings depends on how
    /// it was encoded, and each JSON type takes its own branch: an integer is
    /// ambiguous and offers two, a float is seconds and offers one, a string
    /// may parse either way and offers whichever succeed.
    #[test]
    fn a_sample_timestamp_offers_every_reading_its_encoding_allows() {
        let candidates = |timestamp: serde_json::Value| {
            super::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!([
                timestamp, "1"
            ]))
        };

        // An integer is ambiguous: both the raw value and it read as seconds.
        check!(candidates(serde_json::json!(5)) == Some(vec![5, 5_000_000_000]));
        // Zero collapses to one reading, since both are the same number.
        check!(candidates(serde_json::json!(0)) == Some(vec![0]));

        // A float is seconds, rounded to the nearest nanosecond, and offers
        // only that -- there is no second reading to be ambiguous about.
        check!(candidates(serde_json::json!(5.5)) == Some(vec![5_500_000_000]));
        // Rounded, not truncated. 5.5 lands on a whole nanosecond and cannot
        // show the difference; 1.7 nanoseconds rounds up to 2 where flooring
        // gives 1, which is the sub-nanosecond precision a float carries and
        // an integer count cannot.
        check!(candidates(serde_json::json!(1.7e-9)) == Some(vec![2]));

        // A string is tried both ways and offers whichever parse. "5" has no
        // decimal point so only the integer reading applies; "5.5" is the
        // reverse.
        check!(candidates(serde_json::json!("5")) == Some(vec![5, 5_000_000_000]));
        check!(candidates(serde_json::json!("5.5")) == Some(vec![5_500_000_000]));

        // Nothing parses, or there is nothing to parse.
        check!(candidates(serde_json::json!("nonsense")).is_none());
        check!(candidates(serde_json::json!(true)).is_none());
        check!(
            super::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!([])).is_none()
        );
        check!(
            super::metric_binary_sample_timestamp_ns_candidates(&serde_json::json!("bare"))
                .is_none()
        );
    }

    /// Two samples share an instant if any of their candidate readings agree.
    /// A bare integer is ambiguous -- Prometheus writes timestamps in seconds
    /// and Loki in nanoseconds -- so each yields both readings, and 5 matches
    /// `5_000_000_000` because they are the same moment spelled differently.
    /// That is the whole reason the comparison is over LISTS rather than
    /// values, and a fixture using one spelling throughout never shows it.
    #[test]
    fn two_samples_share_an_instant_if_any_reading_of_them_agrees() {
        let matches = |left, right| super::metric_binary_sample_timestamps_match(&left, &right);
        let at = |timestamp: serde_json::Value| serde_json::json!([timestamp, "1"]);

        // The same number, and the same instant written two ways.
        check!(matches(at(serde_json::json!(5)), at(serde_json::json!(5))));
        check!(
            matches(
                at(serde_json::json!(5)),
                at(serde_json::json!(5_000_000_000_i64))
            ),
            "seconds and nanoseconds for the same moment"
        );
        check!(
            matches(
                at(serde_json::json!(5_000_000_000_i64)),
                at(serde_json::json!(5))
            ),
            "and the other way round"
        );

        // Different instants, in either spelling.
        check!(!matches(at(serde_json::json!(5)), at(serde_json::json!(7))));
        check!(!matches(
            at(serde_json::json!(5)),
            at(serde_json::json!(7_000_000_000_i64))
        ));

        // Neither side parses: they fall back to comparing the raw values, so
        // two identical unparseable timestamps still pair up and two different
        // ones do not.
        check!(matches(
            at(serde_json::json!("nonsense")),
            at(serde_json::json!("nonsense"))
        ));
        check!(!matches(
            at(serde_json::json!("nonsense")),
            at(serde_json::json!("other"))
        ));

        // One side parses and the other does not: no match, rather than
        // falling through to a raw comparison that would never agree anyway.
        check!(!matches(
            at(serde_json::json!(5)),
            at(serde_json::json!("nonsense"))
        ));
        check!(!matches(
            at(serde_json::json!("nonsense")),
            at(serde_json::json!(5))
        ));
    }

    /// `format_metric_value` renders a rational as a decimal, capped at nine
    /// places and with trailing zeros trimmed. A whole number gets no decimal
    /// point at all, which is a different branch from one whose decimals all
    /// trim away -- both are checked, since they produce the same text by
    /// different routes.
    #[test]
    fn a_metric_value_renders_without_trailing_zeros() {
        let render = |numerator, denominator| {
            super::format_metric_value(MetricValue::new(numerator, denominator))
        };

        // Whole numbers take the early return and carry no point.
        check!(render(5, 1) == "5");
        check!(render(0, 1) == "0");
        check!(render(-5, 1) == "-5");
        // A fraction that reduces to a whole number takes the same branch.
        check!(render(10, 5) == "2");

        // Exact decimals keep only the digits they need.
        check!(render(1, 2) == "0.5");
        check!(render(-1, 2) == "-0.5");
        check!(render(1, 4) == "0.25");
        check!(render(3, 2) == "1.5");
        check!(render(-3, 2) == "-1.5");

        // The sign is on the whole part, and survives a zero whole part --
        // "-0.5" rather than "0.5" with the minus lost on the way through
        // `unsigned_abs`.
        check!(render(-1, 4) == "-0.25");

        // A repeating fraction is cut at nine places, not rounded up: a third
        // is nine 3s, and two thirds is nine 6s rather than ...667.
        check!(render(1, 3) == "0.333333333");
        check!(render(2, 3) == "0.666666666");

        // Trailing zeros are trimmed even when the division produces them.
        check!(render(1, 8) == "0.125");
        check!(render(1, 5) == "0.2", "not 0.200000000");

        // The trim only has anything to do when the nine-digit cap lands on a
        // zero: a terminating fraction stops as soon as the remainder does, so
        // it never appends one. 1/11 is 0.090909090... -- nine digits ending
        // in a zero that must come off.
        check!(render(1, 11) == "0.09090909");
    }

    /// `strip_outer_parenthesized_expression` unwraps a query that is wholly
    /// parenthesised, and refuses one that merely starts and ends with
    /// brackets belonging to different groups -- "(a)+(b)" is not a
    /// parenthesised expression, and unwrapping it would produce "a)+(b".
    #[test]
    fn only_a_wholly_parenthesised_expression_is_unwrapped() {
        let strip = super::strip_outer_parenthesized_expression;

        check!(strip("(a)") == Some("a"));
        check!(strip("  (a)  ") == Some("a"), "the query is trimmed first");
        check!(strip("( a )") == Some("a"), "and so are the contents");
        check!(strip("((a))") == Some("(a)"), "one layer at a time");
        check!(strip("(a+b)") == Some("a+b"));

        // The brackets must be the SAME pair. This is the case that a naive
        // starts-with/ends-with check gets wrong.
        check!(strip("(a)+(b)").is_none());
        check!(strip("(a)(b)").is_none());

        // Not parenthesised at all. "a(b)" matters most: it ends with a
        // bracket whose opener is not the first character, so a precheck
        // requiring only ONE of the two ends to match would unwrap it to the
        // nonsense "(b".
        check!(strip("a(b)").is_none());
        check!(strip("a").is_none());
        check!(strip("(a").is_none());
        check!(strip("a)").is_none());
        check!(strip("").is_none());

        // Unbalanced inside. Note the `checked_sub` guarding the depth counter
        // is unreachable: a leading `)` would need the opening precheck to have
        // passed, which requires a leading `(`. Replacing it with a saturating
        // subtraction is an equivalent mutation, not a gap.
        check!(strip("(a))").is_none());
        check!(strip("((a)").is_none());

        // A parenthesis inside a string is text, not structure.
        check!(strip(r#"({app="("})"#) == Some(r#"{app="("}"#));
    }

    /// `MetricValue::sqrt` returns zero rather than an error for anything with
    /// no real root, and it FLOORS to nine decimal places rather than rounding
    /// -- so an irrational root is truncated, not nudged up. A NaN reaching a
    /// series would poison every aggregation over it.
    ///
    /// The `!is_finite() || <= 0.0` guard cannot be tested from outside, and
    /// is kept for what it says rather than what it does: every input it
    /// catches also reaches zero through the fall-through, because
    /// `i128::from_f64(NaN)` defaults to 0 and `MetricValue::new` maps a zero
    /// numerator to zero. Relaxing or removing the guard is an equivalent
    /// mutation. It stays because it states the intent -- no real root means
    /// zero -- where the fall-through only arrives there by accident.
    #[test]
    fn a_metric_square_root_floors_and_refuses_what_has_no_root() {
        let value = |numerator, denominator| MetricValue::new(numerator, denominator);

        check!(value(4, 1).sqrt() == value(2, 1));
        check!(value(9, 1).sqrt() == value(3, 1));
        check!(value(1, 4).sqrt() == value(1, 2), "a fractional root");

        // sqrt(2) is irrational: floored at nine places, not rounded. The
        // tenth digit is a 3, so flooring and rounding agree here -- and
        // sqrt(3) at 1.732050807... has a 5 next, where they differ.
        check!(value(2, 1).sqrt() == MetricValue::new(1_414_213_562, METRIC_DECIMAL_SCALE));
        check!(value(3, 1).sqrt() == MetricValue::new(1_732_050_807, METRIC_DECIMAL_SCALE));

        // Zero and negatives have no positive root, and both answer zero
        // rather than propagating a NaN into the series.
        check!(value(0, 1).sqrt() == MetricValue::zero());
        check!(value(-4, 1).sqrt() == MetricValue::zero());
        check!(value(-1, 1).sqrt() == MetricValue::zero());
    }

    /// `MetricValue::subtract` is exact rational arithmetic, so it must not
    /// round-trip through a float. The operands are chosen with different
    /// denominators, since equal ones let the cross-multiplication cancel out
    /// and hide a swapped operand.
    #[test]
    fn a_metric_subtraction_stays_exact_across_denominators() {
        let value = |numerator, denominator| MetricValue::new(numerator, denominator);

        check!(value(5, 1).subtract(value(3, 1)) == value(2, 1));
        check!(
            value(3, 1).subtract(value(5, 1)) == value(-2, 1),
            "and the other way"
        );

        // 1/2 - 1/3 is exactly 1/6, which no float can hold.
        check!(value(1, 2).subtract(value(1, 3)) == value(1, 6));
        check!(value(1, 3).subtract(value(1, 2)) == value(-1, 6));

        // Subtracting from itself is zero however it is spelled.
        check!(value(7, 3).subtract(value(7, 3)) == MetricValue::zero());
        check!(value(2, 4).subtract(value(1, 2)) == MetricValue::zero());
    }

    /// `sort_loki_stream_values` orders each stream's entries by timestamp.
    /// The timestamps are decimal strings, so a lexicographic sort would put
    /// "1000" before "999" -- the fixture crosses that boundary deliberately.
    /// An unparseable timestamp sorts last rather than first, so a malformed
    /// entry does not claim to be the oldest line in the stream.
    #[test]
    fn loki_stream_values_sort_numerically_not_lexicographically() {
        let entry = |timestamp: &str| [timestamp.to_string(), "line".to_string()];
        let mut streams = BTreeMap::new();
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        streams.insert(
            labels.clone(),
            vec![
                entry("1000"),
                entry("999"),
                entry("nonsense"),
                entry("10000"),
                entry("2"),
            ],
        );

        super::sort_loki_stream_values(&mut streams);

        let order = streams[&labels]
            .iter()
            .map(|[timestamp, _]| timestamp.as_str())
            .collect::<Vec<_>>();
        check!(
            order == vec!["2", "999", "1000", "10000", "nonsense"],
            "numeric order, with the unparseable entry last"
        );
    }

    /// `decode_form_component` decodes one `application/x-www-form-urlencoded`
    /// field: `+` is a space, `%XX` is a byte, and everything else is itself.
    /// A truncated or malformed escape is an error rather than a literal `%`,
    /// and the decoded bytes still have to be UTF-8 -- a valid escape can name
    /// a byte that is not.
    #[test]
    fn a_form_component_decodes_its_escapes_or_refuses_them() {
        let decode = |value: &str| super::decode_form_component(value).ok();

        check!(decode("plain") == Some("plain".to_string()));
        check!(decode("") == Some(String::new()));
        check!(decode("a+b") == Some("a b".to_string()), "plus is a space");
        check!(decode("a%20b") == Some("a b".to_string()), "and so is %20");
        check!(decode("%2F") == Some("/".to_string()));
        check!(
            decode("%2f") == Some("/".to_string()),
            "hex is case-insensitive"
        );
        check!(
            decode("%C3%A9") == Some("\u{e9}".to_string()),
            "a multi-byte character"
        );

        // A `%` that does not introduce two hex digits is an error, not a
        // literal percent sign -- at the end of the string and mid-string.
        check!(decode("a%").is_none());
        check!(decode("a%2").is_none());
        check!(decode("a%ZZb").is_none());
        check!(decode("100%").is_none());

        // A well-formed escape naming a byte that is not valid UTF-8.
        check!(decode("%FF").is_none());
    }

    /// `has_word_boundary` asks whether a match at `index` stands alone rather
    /// than sitting inside a longer word. Both sides have to hold, so each is
    /// broken on its own -- and the ends of the string count as boundaries,
    /// which is what `is_none_or` is doing there.
    #[test]
    fn a_word_boundary_needs_whitespace_or_an_end_on_both_sides() {
        let boundary = super::has_word_boundary;

        check!(boundary("a and b", 2, 3), "space either side");
        check!(boundary("and", 0, 3), "both ends of the string");
        check!(boundary("and b", 0, 3), "the start, and a space after");
        check!(boundary("a and", 2, 3), "a space before, and the end");

        // Each side broken on its own.
        check!(!boundary("aand b", 1, 3), "no boundary before");
        check!(!boundary("a andb", 2, 3), "no boundary after");
        check!(!boundary("aandb", 1, 3), "neither side");
    }

    /// `line_number` counts the newlines before a position, one-based, and
    /// clamps a position past the end rather than panicking on it -- a parse
    /// error can report a position at the very end of the input.
    #[test]
    fn a_line_number_counts_from_one_and_clamps_past_the_end() {
        let line = super::line_number;

        check!(line("abc", 0) == 1, "the first line is one, not zero");
        check!(line("abc", 3) == 1);
        check!(line("a\nb", 0) == 1);
        check!(line("a\nb", 2) == 2, "past the newline");
        check!(line("a\nb", 1) == 1, "the newline itself is still line one");
        check!(line("a\n\nb", 3) == 3, "a blank line counts");
        check!(line("a\nb", 99) == 2, "a position past the end clamps");
        check!(line("", 0) == 1);
        check!(line("", 99) == 1);
    }

    /// `parse_metric_arithmetic_operator` names the six `PromQL` scalar
    /// operators. The variants are asserted pairwise distinct, so an arm
    /// returning a neighbour's operator cannot pass -- and every unrecognised
    /// spelling is refused rather than defaulted, since a silent default here
    /// would compute the wrong arithmetic instead of failing the query.
    #[test]
    fn every_promql_scalar_operator_parses_to_its_own_variant() {
        let parse = super::parse_metric_arithmetic_operator;

        check!(parse("+") == Some(MetricScalarArithmeticOp::Add));
        check!(parse("-") == Some(MetricScalarArithmeticOp::Subtract));
        check!(parse("*") == Some(MetricScalarArithmeticOp::Multiply));
        check!(parse("/") == Some(MetricScalarArithmeticOp::Divide));
        check!(parse("%") == Some(MetricScalarArithmeticOp::Modulo));
        check!(parse("^") == Some(MetricScalarArithmeticOp::Power));

        // Nothing else parses, including operators PromQL has elsewhere.
        check!(parse("").is_none());
        check!(parse("**").is_none());
        check!(parse("+ ").is_none(), "the operator is not trimmed here");
        check!(parse("and").is_none());
        check!(parse("==").is_none(), "a comparison is not arithmetic");

        let variants = [
            parse("+"),
            parse("-"),
            parse("*"),
            parse("/"),
            parse("%"),
            parse("^"),
        ];
        for (index, left) in variants.iter().enumerate() {
            for right in &variants[index + 1..] {
                check!(left != right, "two operators share a variant: {left:?}");
            }
        }
    }

    /// `split_leading_vector_group_modifier` peels a `group_left`/`group_right`
    /// off the front of a vector-match clause, with or without a label list.
    /// Four routes leave the function and each returns a different shape, so
    /// each is pinned: no modifier, a bare one, one with labels, one with an
    /// empty list, and an unclosed list -- which returns the query untouched
    /// rather than a half-parsed modifier.
    #[test]
    fn a_leading_vector_group_modifier_is_peeled_with_its_labels() {
        let split = super::split_leading_vector_group_modifier;

        // No modifier: the query comes back whole.
        check!(split("foo") == (None, "foo"));
        check!(split("  foo") == (None, "foo"), "but trimmed at the front");
        check!(split("") == (None, ""));

        // A bare modifier, with the remainder handed back trimmed.
        check!(split("group_left foo") == (Some("group_left".to_string()), "foo"));
        check!(split("group_right foo") == (Some("group_right".to_string()), "foo"));

        // With labels, which are folded into the modifier's own text.
        check!(
            split("group_left(instance) foo")
                == (Some("group_left (instance)".to_string()), " foo")
        );
        check!(split("group_right(a,b) foo") == (Some("group_right (a,b)".to_string()), " foo"));

        // An empty label list is the bare modifier again, not "group_left ()".
        check!(split("group_left() foo") == (Some("group_left".to_string()), " foo"));

        // An unclosed label list is not a modifier at all: the query is
        // returned untouched rather than half-consumed.
        check!(split("group_left(instance foo") == (None, "group_left(instance foo"));

        // The match is a bare prefix test, not a word match, so a longer
        // identifier starting with a modifier name is split mid-word. That is
        // current behaviour rather than obviously desirable, and it is pinned
        // so a change to it is deliberate.
        //
        // The order the two modifiers are tried in cannot matter: neither is
        // a prefix of the other, so at most one can ever strip. Swapping them
        // is an equivalent mutation, not an untested one.
        check!(split("group_rightish") == (Some("group_right".to_string()), "ish"));
    }

    /// A `Prometheus` alert is PENDING until it has been continuously active for
    /// its `for` duration, then FIRING. The transition is at `>=`, so an alert
    /// exactly at its hold duration is already firing -- one nanosecond either
    /// side of that instant is the only pair separating `>=` from `>`.
    ///
    /// `active_at` is remembered across evaluations, which is what makes the
    /// duration a duration rather than a single-evaluation check: the same
    /// alert is evaluated three times here against one shared state.
    #[test]
    fn an_alert_fires_once_it_has_held_for_its_configured_duration() {
        let states = super::SharedPrometheusAlertStates::default();
        let fields: serde_yaml::Mapping =
            serde_yaml::from_str("for: 5m\n").expect("the rule fields parse");
        let result = serde_json::json!({
            "data": {
                "result": [{
                    "metric": {"job": "api"},
                    "value": [0, "1"],
                }],
            }
        });
        let hold_ns = 5 * 60 * 1_000_000_000_i64;
        let started = 1_000_000_000_000_i64;
        let evaluate = |at| {
            super::prometheus_alerts_from_query_result(
                &states,
                "tenant",
                "HighErrors",
                &fields,
                "up",
                at,
                &result,
            )
        };
        let state_at = |at| {
            let alerts = evaluate(at);
            check!(alerts.len() == 1, "one sample means one alert");
            alerts[0]["state"].as_str().expect("a state").to_string()
        };

        // First evaluation starts the clock: nothing has held yet.
        check!(state_at(started) == "pending");

        // One nanosecond short of the hold duration is still pending, and
        // exactly at it is firing. Those two evaluations are the test.
        check!(state_at(started + hold_ns - 1) == "pending");
        check!(state_at(started + hold_ns) == "firing");
        check!(state_at(started + hold_ns + 1) == "firing");

        // The alert carries the labels of its sample plus its own name, and
        // reports the value the query returned.
        let alerts = evaluate(started + hold_ns);
        check!(alerts[0]["labels"]["job"] == "api");
        check!(
            alerts[0]["labels"]["alertname"] == "HighErrors",
            "the rule's name is added to the sample's labels"
        );
        check!(alerts[0]["value"] == "1");

        // A rule with no `for` fires on its first evaluation, since a zero
        // hold duration is satisfied immediately.
        let immediate = super::SharedPrometheusAlertStates::default();
        let no_hold: serde_yaml::Mapping =
            serde_yaml::from_str("severity: page\n").expect("the rule fields parse");
        let alerts = super::prometheus_alerts_from_query_result(
            &immediate,
            "tenant",
            "Immediate",
            &no_hold,
            "up",
            started,
            &result,
        );
        check!(alerts[0]["state"] == "firing");
    }

    /// Evaluating one rule prunes the alert states belonging to *it*, and
    /// leaves every other rule's alone. The three identity fields are or-ed,
    /// so a state is kept when any one of them differs -- and each has to be
    /// the only difference, or a mutant that requires two of them would still
    /// keep it.
    #[test]
    fn evaluating_one_alert_rule_leaves_the_other_rules_states_alone() {
        let states = super::SharedPrometheusAlertStates::default();
        let fields: serde_yaml::Mapping =
            serde_yaml::from_str("severity: page\n").expect("the rule fields parse");
        let result = serde_json::json!({
            "data": { "result": [{ "metric": {"job": "api"}, "value": [0, "1"] }] }
        });
        let evaluate = |tenant: &str, alert: &str, query: &str| {
            super::prometheus_alerts_from_query_result(
                &states, tenant, alert, &fields, query, 1_000, &result,
            );
        };
        let held = || {
            states
                .alerts
                .lock()
                .expect("the alert states lock is not poisoned")
                .keys()
                .map(|key| {
                    (
                        key.tenant.clone(),
                        key.alert_name.clone(),
                        key.query.clone(),
                    )
                })
                .collect::<Vec<_>>()
        };

        evaluate("t1", "A", "up");
        check!(held().len() == 1);

        // Each of these differs from the first in exactly one field, and none
        // of them may disturb it.
        evaluate("t2", "A", "up");
        evaluate("t1", "B", "up");
        evaluate("t1", "A", "down");

        let mut keys = held();
        keys.sort();
        check!(
            keys == vec![
                ("t1".to_string(), "A".to_string(), "down".to_string()),
                ("t1".to_string(), "A".to_string(), "up".to_string()),
                ("t1".to_string(), "B".to_string(), "up".to_string()),
                ("t2".to_string(), "A".to_string(), "up".to_string()),
            ],
            "got {keys:?}"
        );
    }

    /// `signed_vector_function_literal_error` catches `vector(+1)`, which `LogQL`
    /// does not accept -- the argument must be a bare number. It skips any
    /// whitespace after the parenthesis before looking, so the reported column
    /// is the SIGN's, not the parenthesis's, and the message names which sign
    /// was found.
    ///
    /// As with the unspaced-operator detector, the column counts characters:
    /// one case puts multi-byte text ahead of the call so a byte offset gives
    /// a different number.
    #[test]
    fn a_signed_vector_literal_is_reported_at_the_sign() {
        let error = super::signed_vector_function_literal_error;
        let column = |query: &str| {
            error(query).map(|message| {
                message
                    .split("col ")
                    .nth(1)
                    .and_then(|rest| rest.split(':').next())
                    .expect("the message names a column")
                    .parse::<usize>()
                    .expect("the column is a number")
            })
        };

        check!(column("vector(+1)") == Some(8));
        check!(column("vector(-1)") == Some(8));

        // Whitespace after the parenthesis is skipped, so the column follows
        // the sign rather than sitting on the bracket.
        check!(column("vector( +1)") == Some(9));
        check!(column("vector(   -1)") == Some(11));

        // The message names the sign it found, not a fixed one.
        check!(
            error("vector(+1)")
                .expect("a signed literal is an error")
                .contains("unexpected +, expecting NUMBER")
        );
        check!(
            error("vector(-1)")
                .expect("a signed literal is an error")
                .contains("unexpected -, expecting NUMBER")
        );

        // An unsigned argument is fine, and so is anything that is not a sign.
        check!(error("vector(1)").is_none());
        check!(error("vector( 1)").is_none());
        check!(error("vector(x)").is_none());
        check!(error("vector()").is_none());

        // Characters, not bytes: fourteen characters precede the sign here
        // but sixteen bytes do.
        check!(column("(\"\u{e9}\u{e9}\")+vector(-1)") == Some(15));

        // A `vector(` inside a string is text.
        check!(error("(\"vector(+1)\")").is_none());

        // The parenthesis is part of the match, not assumed to follow. Without
        // it, "vector -1" would land the offset straight on the minus and
        // report a signed literal for a call that was never made.
        check!(error("vector -1").is_none());
        check!(error("vector_total -1").is_none());
    }

    /// `unspaced_vector_set_operator_error` catches `)and` written without a
    /// space -- a `LogQL` typo that would otherwise fail somewhere unhelpful --
    /// and reports the column the operator starts at.
    ///
    /// That column is a CHARACTER count, not a byte offset, so one case puts
    /// multi-byte text before the parenthesis: with ASCII alone the two are
    /// the same number and a byte count passes.
    #[test]
    fn an_unspaced_set_operator_is_reported_at_its_own_column() {
        let error = super::unspaced_vector_set_operator_error;
        let column = |query: &str| {
            error(query).map(|message| {
                message
                    .split("col ")
                    .nth(1)
                    .and_then(|rest| rest.split(':').next())
                    .expect("the message names a column")
                    .parse::<usize>()
                    .expect("the column is a number")
            })
        };

        // All three operators, each glued to the closing parenthesis.
        check!(column("vector(1)and vector(2)") == Some(10));
        check!(column("vector(1)or vector(2)") == Some(10));
        check!(column("vector(1)unless vector(2)") == Some(10));

        // Properly spaced is not an error.
        check!(error("vector(1) and vector(2)").is_none());
        check!(error("vector(1)").is_none());

        // A closing parenthesis followed by anything else is fine.
        check!(error("vector(1)+vector(2)").is_none());

        // Unlike the set-operator SPLITTER, this detector has no word-boundary
        // test, so `)android` is reported as an unspaced `and`. That is a
        // false positive, but it fires only on a query that is already a
        // syntax error, so it turns one bad message into a better-placed one.
        // Pinned because it is behaviour, not because it is desirable.
        check!(column("vector(1)android") == Some(10));

        // The column counts characters. Six characters precede the operator
        // here but eight bytes do, because each accented letter takes two.
        check!(column("(\"\u{e9}\u{e9}\")and 1") == Some(7));

        // A `)and` inside a string is text, not an operator.
        check!(error("vector(1)").is_none());
        check!(error("(\")and\")").is_none());

        // The check only applies to scalar-vector expressions: an aggregation
        // is parsed elsewhere and must not be second-guessed here.
        check!(error("sum(rate(x[5m]))and y").is_none());
    }

    /// `format_vector_aggregation_query` renders an aggregation back to its
    /// `LogQL` spelling. Most operators take an optional grouping clause, but
    /// three -- `approx_topk`, sort and `sort_desc` -- have no grouped form and
    /// must refuse rather than render one, so each is checked BOTH ways.
    ///
    /// The two limit-taking operators put their limit inside the parentheses
    /// ahead of the inner query, where the ungrouped ones do not, which is why
    /// the names alone are not enough to pin them.
    #[test]
    fn a_vector_aggregation_renders_only_the_groupings_its_operator_allows() {
        use krabka_logql::{VectorAggregation, VectorAggregationOp, VectorGrouping};

        let render = |op, grouping| {
            super::format_vector_aggregation_query(&VectorAggregation { op, grouping }, "up")
        };
        let by = || {
            Some(VectorGrouping::By(vec![
                "job".to_string(),
                "app".to_string(),
            ]))
        };
        let without = || Some(VectorGrouping::Without(vec!["pod".to_string()]));

        // Plain operators, ungrouped and grouped both ways.
        check!(render(VectorAggregationOp::Sum, None) == Some("sum(up)".to_string()));
        check!(render(VectorAggregationOp::Count, None) == Some("count(up)".to_string()));
        check!(render(VectorAggregationOp::Min, None) == Some("min(up)".to_string()));
        check!(render(VectorAggregationOp::Max, None) == Some("max(up)".to_string()));
        check!(render(VectorAggregationOp::Avg, None) == Some("avg(up)".to_string()));
        check!(render(VectorAggregationOp::Stddev, None) == Some("stddev(up)".to_string()));
        check!(render(VectorAggregationOp::Stdvar, None) == Some("stdvar(up)".to_string()));

        // The grouping is joined with a comma and sits before the parentheses.
        check!(render(VectorAggregationOp::Sum, by()) == Some("sum by (job,app)(up)".to_string()));
        check!(
            render(VectorAggregationOp::Max, without())
                == Some("max without (pod)(up)".to_string())
        );

        // The limit-taking operators put their limit inside, before the inner
        // query, and still accept a grouping.
        check!(render(VectorAggregationOp::TopK(3), None) == Some("topk(3,up)".to_string()));
        check!(render(VectorAggregationOp::BottomK(3), None) == Some("bottomk(3,up)".to_string()));
        check!(
            render(VectorAggregationOp::TopK(5), by())
                == Some("topk by (job,app)(5,up)".to_string())
        );

        // These three have no grouped form: rendered ungrouped, refused with
        // a grouping. Both directions matter -- a mutant that dropped the
        // guard would render an expression LogQL cannot parse back.
        check!(render(VectorAggregationOp::Sort, None) == Some("sort(up)".to_string()));
        check!(render(VectorAggregationOp::Sort, by()).is_none());
        check!(render(VectorAggregationOp::SortDesc, None) == Some("sort_desc(up)".to_string()));
        check!(render(VectorAggregationOp::SortDesc, without()).is_none());
        check!(
            render(VectorAggregationOp::ApproxTopK(4), None)
                == Some("approx_topk(4,up)".to_string())
        );
        check!(render(VectorAggregationOp::ApproxTopK(4), by()).is_none());

        // count_values has no rendering at all, grouped or not.
        check!(render(VectorAggregationOp::CountValues("x".to_string()), None).is_none());
        check!(render(VectorAggregationOp::CountValues("x".to_string()), by()).is_none());
    }

    /// Vector arithmetic replaces each sample's value with `left op right`,
    /// keeping only the timestamps both sides carry. The operator is
    /// non-commutative here on purpose: subtraction and division both give a
    /// different answer with the operands swapped, which a fixture using only
    /// `+` or `*` could never show.
    ///
    /// Like its comparison twin, this removes in place at two sites and the
    /// index must not advance on either, so the dropped samples are adjacent.
    #[test]
    fn vector_arithmetic_computes_left_op_right_where_both_have_a_sample() {
        use krabka_logql::MetricScalarArithmeticOp;

        let series = |samples: &[(i64, &str)]| {
            serde_json::json!({
                "metric": {"app": "api"},
                "values": samples
                    .iter()
                    .map(|(ts, value)| serde_json::json!([ts, value]))
                    .collect::<Vec<_>>(),
            })
        };
        let pairs = |value: &serde_json::Value| {
            value
                .get("values")
                .and_then(serde_json::Value::as_array)
                .expect("the series has values")
                .iter()
                .map(|sample| {
                    (
                        sample[0].as_i64().expect("a timestamp"),
                        sample[1].as_str().expect("a value").to_string(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let left = series(&[(1, "10"), (4, "20"), (5, "20")]);
        // 2, 3 and 6 have no left sample; 2 and 3 are adjacent.
        let right = || series(&[(1, "2"), (2, "1"), (3, "1"), (4, "5"), (5, "5"), (6, "1")]);
        let apply = |op| {
            let mut output = right();
            let kept = super::apply_metric_binary_arithmetic_to_series_with_left_operand(
                &mut output,
                &left,
                op,
            );
            (kept, pairs(&output))
        };

        // 10-2, 20-5, 20-5 -- not 2-10, which is what a swap would give.
        check!(
            apply(MetricScalarArithmeticOp::Subtract)
                == (
                    true,
                    vec![
                        (1, "8".to_string()),
                        (4, "15".to_string()),
                        (5, "15".to_string()),
                    ]
                )
        );
        check!(
            apply(MetricScalarArithmeticOp::Divide)
                == (
                    true,
                    vec![
                        (1, "5".to_string()),
                        (4, "4".to_string()),
                        (5, "4".to_string()),
                    ]
                )
        );
        check!(
            apply(MetricScalarArithmeticOp::Multiply)
                == (
                    true,
                    vec![
                        (1, "20".to_string()),
                        (4, "100".to_string()),
                        (5, "100".to_string()),
                    ]
                )
        );

        // A division with no answer drops its sample rather than emitting one.
        let mut output = series(&[(1, "0")]);
        check!(
            !super::apply_metric_binary_arithmetic_to_series_with_left_operand(
                &mut output,
                &left,
                MetricScalarArithmeticOp::Divide,
            )
        );

        // The instant shape again, where nothing pre-matches the timestamps.
        let instant =
            |ts: i64, value: &str| serde_json::json!({"metric": {}, "value": [ts, value]});
        let mut output = instant(1, "2");
        check!(
            super::apply_metric_binary_arithmetic_to_series_with_left_operand(
                &mut output,
                &instant(1, "10"),
                MetricScalarArithmeticOp::Subtract,
            )
        );
        check!(output["value"][1] == "8");

        let mut output = instant(1, "2");
        check!(
            !super::apply_metric_binary_arithmetic_to_series_with_left_operand(
                &mut output,
                &instant(2, "10"),
                MetricScalarArithmeticOp::Subtract,
            ),
            "two different instants have no arithmetic between them"
        );
    }

    /// A `PromQL` comparison between two vectors drops the samples that fail it
    /// and gives the survivors the LEFT operand's value -- the comparison is a
    /// filter, not a rewrite to a boolean. With the `bool` modifier it becomes
    /// the opposite: nothing is dropped and every value becomes "1" or "0".
    /// Both modes are checked over the same pair, since a mutant that ignores
    /// the modifier agrees with whichever mode it happens to implement.
    ///
    /// Samples are removed in place from two different sites -- one for a
    /// timestamp the left side lacks, one for a failed comparison -- and
    /// neither may advance the index. So the fixture drops an ADJACENT PAIR at
    /// each site: a lone drop cannot show a skipped neighbour.
    #[test]
    fn a_vector_comparison_filters_and_takes_the_left_operand() {
        use krabka_logql::ComparisonOp;

        let series = |samples: &[(i64, &str)]| {
            serde_json::json!({
                "metric": {"app": "api"},
                "values": samples
                    .iter()
                    .map(|(ts, value)| serde_json::json!([ts, value]))
                    .collect::<Vec<_>>(),
            })
        };
        let pairs = |value: &serde_json::Value| {
            value
                .get("values")
                .and_then(serde_json::Value::as_array)
                .expect("the series has values")
                .iter()
                .map(|sample| {
                    (
                        sample[0].as_i64().expect("a timestamp"),
                        sample[1].as_str().expect("a value").to_string(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let left = series(&[(1, "10"), (4, "20"), (5, "20")]);
        // 2 and 3 have no left sample; 4 and 5 fail the comparison. Each pair
        // is adjacent, so an index that advanced on removal would keep one.
        let right = || series(&[(1, "1"), (2, "1"), (3, "1"), (4, "30"), (5, "30"), (6, "1")]);

        let mut output = right();
        check!(
            super::apply_metric_binary_comparison_to_series_with_left_operand(
                &mut output,
                &left,
                ComparisonOp::Greater,
                false,
            )
        );
        check!(
            pairs(&output) == vec![(1, "10".to_string())],
            "only 10 > 1 survives, carrying the LEFT value"
        );

        // With `bool`, the failures stay and report 0 -- but a sample the left
        // side never had is still dropped, because there is nothing to compare.
        let mut output = right();
        check!(
            super::apply_metric_binary_comparison_to_series_with_left_operand(
                &mut output,
                &left,
                ComparisonOp::Greater,
                true,
            )
        );
        check!(
            pairs(&output)
                == vec![
                    (1, "1".to_string()),
                    (4, "0".to_string()),
                    (5, "0".to_string()),
                ]
        );

        // The operator is honoured, not assumed: the same pair under `<`
        // keeps exactly the samples `>` dropped.
        let mut output = right();
        check!(
            super::apply_metric_binary_comparison_to_series_with_left_operand(
                &mut output,
                &left,
                ComparisonOp::Less,
                false,
            )
        );
        check!(pairs(&output) == vec![(4, "20".to_string()), (5, "20".to_string())]);

        // Everything filtered out reports false so the caller drops the series.
        let mut output = series(&[(1, "99")]);
        check!(
            !super::apply_metric_binary_comparison_to_series_with_left_operand(
                &mut output,
                &left,
                ComparisonOp::Greater,
                false,
            )
        );

        // A left series with no values at all matches nothing.
        let mut output = right();
        check!(
            !super::apply_metric_binary_comparison_to_series_with_left_operand(
                &mut output,
                &serde_json::json!({"metric": {}}),
                ComparisonOp::Greater,
                false,
            )
        );

        // The instant-vector shape carries one `value`, and nothing pre-matches
        // its timestamp the way the range path does -- so the comparison itself
        // has to refuse two samples from different instants. Comparing them
        // would report a result for a moment neither side observed.
        let instant =
            |ts: i64, value: &str| serde_json::json!({"metric": {}, "value": [ts, value]});
        let mut output = instant(1, "1");
        check!(
            super::apply_metric_binary_comparison_to_series_with_left_operand(
                &mut output,
                &instant(1, "10"),
                ComparisonOp::Greater,
                false,
            ),
            "same instant, and 10 > 1"
        );
        check!(output["value"][1] == "10", "and it takes the left value");

        let mut output = instant(1, "1");
        check!(
            !super::apply_metric_binary_comparison_to_series_with_left_operand(
                &mut output,
                &instant(2, "10"),
                ComparisonOp::Greater,
                false,
            ),
            "different instants do not compare, however the values order"
        );
    }

    /// `apply_metric_binary_set_to_series` filters one series against another
    /// by the set operator. All three operators are applied to the SAME pair,
    /// so each keeps a different subset -- with one operator alone, a rule
    /// that returns a constant looks correct on whichever half it agrees with.
    ///
    /// The filter walks with an index and removes in place, only advancing
    /// when it keeps. A removal that also advanced would skip the sample that
    /// slid into the gap, so the dropped samples are adjacent here.
    #[test]
    fn a_binary_set_operator_keeps_the_subset_it_names() {
        use krabka_logql::MetricBinarySetOp;

        let range = |samples: &[i64]| {
            serde_json::json!({
                "metric": {"app": "api"},
                "values": samples
                    .iter()
                    .map(|ts| serde_json::json!([ts, ts.to_string()]))
                    .collect::<Vec<_>>(),
            })
        };
        let timestamps = |series: &serde_json::Value| {
            series
                .get("values")
                .and_then(serde_json::Value::as_array)
                .expect("the series has values")
                .iter()
                .map(|sample| sample[0].as_i64().expect("a timestamp"))
                .collect::<Vec<_>>()
        };
        // 2 and 3 are adjacent, so `and` and `unless` each drop a run rather
        // than isolated samples.
        let right = range(&[2, 3]);
        let apply = |op| {
            let mut left = range(&[1, 2, 3, 4]);
            let kept = super::apply_metric_binary_set_to_series(&mut left, &right, op);
            (kept, timestamps(&left))
        };

        check!(apply(MetricBinarySetOp::And) == (true, vec![2, 3]));
        check!(apply(MetricBinarySetOp::Unless) == (true, vec![1, 4]));
        check!(apply(MetricBinarySetOp::Or) == (true, vec![1, 2, 3, 4]));

        // When the filter empties the series it reports false, so the caller
        // can drop it rather than emitting an empty series.
        let mut left = range(&[1, 4]);
        check!(!super::apply_metric_binary_set_to_series(
            &mut left,
            &right,
            MetricBinarySetOp::And
        ));
        check!(timestamps(&left).is_empty());

        // `or` keeps a series the right side never matches at all.
        let mut left = range(&[9]);
        check!(super::apply_metric_binary_set_to_series(
            &mut left,
            &right,
            MetricBinarySetOp::Or
        ));

        // An instant vector carries one `value` rather than `values`, and the
        // same three rules apply to it.
        let instant = |ts: i64| serde_json::json!({"metric": {}, "value": [ts, ts.to_string()]});
        let mut matching = instant(5);
        check!(super::apply_metric_binary_set_to_series(
            &mut matching,
            &instant(5),
            MetricBinarySetOp::And
        ));
        let mut differing = instant(5);
        check!(!super::apply_metric_binary_set_to_series(
            &mut differing,
            &instant(6),
            MetricBinarySetOp::And
        ));
        let mut differing = instant(5);
        check!(super::apply_metric_binary_set_to_series(
            &mut differing,
            &instant(6),
            MetricBinarySetOp::Unless
        ));

        // A series with neither shape matches nothing.
        let mut empty = serde_json::json!({"metric": {}});
        check!(!super::apply_metric_binary_set_to_series(
            &mut empty,
            &right,
            MetricBinarySetOp::Or
        ));
    }

    /// `split_top_level_set_query` is the third splitter, over `PromQL`'s set
    /// operators. Unlike the symbol splitters these are WORDS, so a match must
    /// also stand alone: "android" starts with "and" and is not a set
    /// operation. That word-boundary test is the whole difference between this
    /// splitter and the other two.
    ///
    /// Two things here cannot be tested and are not: the order the three
    /// operators are tried in, since none is a prefix of another and the
    /// boundary test applies to each; and the `is_ascii_alphabetic` precheck,
    /// which only skips characters where `starts_with` would fail anyway. Both
    /// are equivalent mutations rather than gaps.
    #[test]
    fn a_top_level_set_split_needs_a_whole_word() {
        let split = super::split_top_level_set_query;

        check!(split("a and b") == Some(("a ", "and", "b")));
        check!(split("a or b") == Some(("a ", "or", "b")));
        check!(split("a unless b") == Some(("a ", "unless", "b")));

        // A word that merely starts with an operator is not one. Each of the
        // three has its own trap, since only a shared boundary test saves
        // them all at once.
        check!(split("android").is_none(), "and is not a prefix match");
        check!(split("orders").is_none());
        check!(split("unlessened").is_none());
        check!(split("a android b").is_none(), "nor mid-query");

        // Nor is one glued to its neighbours without spaces.
        check!(split("aand b").is_none());
        check!(split("a andb").is_none());

        // Nested operators are not top level, and a quoted one is text.
        check!(split("sum(a and b) or c") == Some(("sum(a and b) ", "or", "c")));
        check!(split(r#"{app="a or b"}"#).is_none());
        check!(split("rate(x[5m]) unless y") == Some(("rate(x[5m]) ", "unless", "y")));

        // Nothing to split.
        check!(split("a").is_none());
        check!(split("").is_none());
    }

    /// `split_top_level_arithmetic_query` is the comparison splitter's twin
    /// over the six arithmetic operators. It has the same depth guard, and it
    /// maps the matched character back to a static string -- an arm returning
    /// a neighbour's symbol still produces a valid split, so every operator is
    /// pinned to its own.
    ///
    /// The first operator wins, which matters because these are scanned left
    /// to right with no precedence: "a - b * c" splits at the minus.
    #[test]
    fn a_top_level_arithmetic_split_names_the_operator_it_found() {
        let split = super::split_top_level_arithmetic_query;

        check!(split("a + b") == Some(("a ", "+", "b")));
        check!(split("a - b") == Some(("a ", "-", "b")));
        check!(split("a * b") == Some(("a ", "*", "b")));
        check!(split("a / b") == Some(("a ", "/", "b")));
        check!(split("a % b") == Some(("a ", "%", "b")));
        check!(split("a ^ b") == Some(("a ", "^", "b")));

        // Leftmost wins, with no precedence applied during the split.
        check!(split("a - b * c") == Some(("a ", "-", "b * c")));
        check!(split("a * b - c") == Some(("a ", "*", "b - c")));

        // Nested operators are not top level, in each kind of bracket.
        check!(split("sum(a + b) * 2") == Some(("sum(a + b) ", "*", "2")));
        check!(split("rate(x[5m]) * 2") == Some(("rate(x[5m]) ", "*", "2")));
        check!(split(r#"{app="a-b"} * 2"#) == Some((r#"{app="a-b"} "#, "*", "2")));

        // And an operator inside a quoted string is just text.
        check!(split(r#"{app="a+b"}"#).is_none());
        check!(split("sum(a + b)").is_none());
        check!(split("a").is_none());
    }

    /// `split_top_level_comparison_query` finds the comparison a `PromQL` query
    /// is rooted at, ignoring operators nested inside brackets or quotes. The
    /// depth guard is three counters joined by `&&`, and each has to reject on
    /// its own -- so a matcher inside braces and a comparison inside
    /// parentheses are both checked, each of which a loosened guard would
    /// split at instead.
    #[test]
    fn a_top_level_comparison_ignores_operators_nested_inside_the_query() {
        let split = super::split_top_level_comparison_query;

        // Every operator, and the longest match wins: `>=` is not `>`.
        check!(split("up > 1") == Some(("up ", ">", "1")));
        check!(split("up >= 1") == Some(("up ", ">=", "1")));
        check!(split("up < 1") == Some(("up ", "<", "1")));
        check!(split("up <= 1") == Some(("up ", "<=", "1")));
        check!(split("up == 1") == Some(("up ", "==", "1")));
        check!(split("up != 1") == Some(("up ", "!=", "1")));
        check!(split("up>=1") == Some(("up", ">=", "1")), "without spaces");

        // A label matcher inside braces is not a top-level comparison. This is
        // the case the brace counter exists for: loosening the guard splits
        // the query at the matcher's own `!=` and leaves a broken left side.
        check!(split(r#"{app!="a"} > 1"#) == Some((r#"{app!="a"} "#, ">", "1")));

        // Nor is a comparison inside parentheses -- the outer one wins.
        check!(split("(a > b) > 2") == Some(("(a > b) ", ">", "2")));

        // Nor one inside a quoted string, which the quote tracking skips.
        check!(split(r#"{app="x>y"} > 1"#) == Some((r#"{app="x>y"} "#, ">", "1")));

        // A range selector's brackets nest too.
        check!(split("sum(rate(up[5m])) > 0.5") == Some(("sum(rate(up[5m])) ", ">", "0.5")));

        // The bracket counter is defensive: no real range selector contains a
        // comparison, so nothing valid exercises it. This input is not a
        // PromQL query, but the scanner takes any string, and the counter's
        // whole purpose is to not split inside brackets.
        check!(split("a[>]b > 1") == Some(("a[>]b ", ">", "1")));

        // No top-level comparison at all.
        check!(split("up").is_none());
        check!(split("sum(rate(up[5m]))").is_none());
        check!(
            split(r#"{app!="a"}"#).is_none(),
            "a matcher alone is not one"
        );
    }

    /// `format_loki_offset_duration_ns` spells a duration the way `Loki` does,
    /// picking the largest unit that fits. Each `>=` is the boundary between
    /// two units, so each is checked exactly at its own threshold and one
    /// step below it -- a `<` there sends the value to the next unit down.
    #[test]
    fn a_loki_offset_duration_picks_the_largest_unit_that_fits() {
        let format = super::format_loki_offset_duration_ns;

        // Zero is a duration, not an absence. A `<= 0` guard would lose it.
        check!(format(0) == Some("0s".to_string()));
        // Negative is an absence, which is what separates `< 0` from `== 0`.
        check!(format(-1).is_none());
        check!(format(-3_600_000_000_000).is_none());

        // At and just below the seconds boundary.
        check!(format(1_000_000_000) == Some("1s".to_string()));
        check!(format(1_500_000_000) == Some("1.5s".to_string()));
        check!(format(999_999_999) == Some("999.999999ms".to_string()));

        // At and just below the milliseconds boundary.
        check!(format(1_000_000) == Some("1ms".to_string()));
        check!(format(999_999) == Some("999.999\u{00b5}s".to_string()));

        // At and just below the microseconds boundary.
        check!(format(1_000) == Some("1\u{00b5}s".to_string()));
        check!(format(999) == Some("999ns".to_string()));

        // Larger units compose rather than replacing one another.
        check!(format(3_600_000_000_000) == Some("1h0m0s".to_string()));
        check!(format(90_000_000_000) == Some("1m30s".to_string()));
    }

    /// `apply_loki_stream_limit` spends one budget across several streams,
    /// truncating the last one that fits. The budget only visibly decrements
    /// when an earlier stream takes part of it and a later stream needs the
    /// rest -- with a single stream, any arithmetic on the remainder looks
    /// alike.
    #[test]
    fn a_loki_stream_limit_is_spent_across_streams_in_order() {
        let streams = |counts: &[usize]| {
            serde_json::json!({
                "data": {
                    "resultType": "streams",
                    "result": counts
                        .iter()
                        .map(|count| serde_json::json!({
                            "stream": {"app": "a"},
                            "values": (0..*count)
                                .map(|i| serde_json::json!([i.to_string(), "line"]))
                                .collect::<Vec<_>>(),
                        }))
                        .collect::<Vec<_>>(),
                }
            })
        };
        let kept = |value: &serde_json::Value| {
            value
                .pointer("/data/result")
                .and_then(serde_json::Value::as_array)
                .expect("the result is an array")
                .iter()
                .map(|stream| {
                    stream
                        .get("values")
                        .and_then(serde_json::Value::as_array)
                        .map_or(0, Vec::len)
                })
                .collect::<Vec<_>>()
        };

        // The first stream takes 2 of the 5, leaving 3 for the second.
        // Adding instead would leave 7, and dividing would leave 2.
        check!(kept(&super::apply_loki_stream_limit(streams(&[2, 10]), Some(5))) == vec![2, 3]);

        // A stream that exhausts the budget empties every stream after it,
        // and emptied streams are dropped entirely.
        check!(kept(&super::apply_loki_stream_limit(streams(&[5, 10]), Some(5))) == vec![5]);

        // Under budget, nothing is touched.
        check!(kept(&super::apply_loki_stream_limit(streams(&[2, 2]), Some(5))) == vec![2, 2]);

        // No limit means no truncation, and a non-streams result is left alone.
        check!(kept(&super::apply_loki_stream_limit(streams(&[9]), None)) == vec![9]);
    }

    /// The two `LogQL` token namers turn a parser's own wording into the token
    /// names `Loki`'s clients expect. Each named arm falls through to a generic
    /// rewrite when deleted, so every one is pinned to its own answer.
    #[test]
    fn logql_parse_errors_name_the_tokens_loki_clients_expect() {
        let expected = super::expected_logql_token;
        let unexpected = super::unexpected_logql_token;

        check!(expected("expected '\"'") == "STRING");
        check!(expected("expected closing quote") == "STRING");
        check!(expected("expected label matcher operator") == "ASSIGN, EQ, NEQ, RE, NRE");
        check!(expected("expected label name") == "IDENTIFIER");
        check!(expected("expected end of query") == "$end");

        // Anything else keeps its wording with the lead-in stripped, which is
        // what a deleted arm above would fall through to.
        check!(expected("expected a pipeline stage") == "a pipeline stage");
        check!(expected("something else entirely") == "something else entirely");

        // An underscore starts an identifier just as a letter does, which is
        // the case separating `==` from `!=` in that test.
        check!(unexpected("_foo", 0) == "IDENTIFIER");
        check!(unexpected("foo", 0) == "IDENTIFIER");
        check!(
            unexpected("{app=\"a\"}", 0) == "{",
            "punctuation names itself"
        );
        check!(
            unexpected("1", 0) == "1",
            "and a digit is not an identifier"
        );
        check!(unexpected("", 0) == "$end");
        check!(
            unexpected("abc", 99) == "$end",
            "a position past the end is the end"
        );
    }

    /// `hex_value` maps a hex digit to its value across three ranges. Every
    /// range boundary is checked together with the character immediately
    /// outside it, since a range widened or narrowed by one is invisible from
    /// the middle -- and the two letter ranges must not be confused, because
    /// their offsets differ by the distance between the cases.
    #[test]
    fn hex_digits_map_across_all_three_ranges_and_nothing_else() {
        let value = super::hex_value;

        check!(value(b'0') == Some(0), "the low edge of the digits");
        check!(value(b'9') == Some(9), "and the high edge");
        check!(value(b'5') == Some(5));
        check!(value(b'a') == Some(10), "lower-case a continues from nine");
        check!(value(b'f') == Some(15));
        check!(value(b'A') == Some(10), "upper-case is the same value");
        check!(value(b'F') == Some(15));

        // One character outside each range, on both sides.
        check!(value(b'/') == None, "just below '0'");
        check!(value(b':') == None, "just above '9'");
        check!(value(b'`') == None, "just below 'a'");
        check!(value(b'g') == None, "just above 'f'");
        check!(value(b'@') == None, "just below 'A'");
        check!(value(b'G') == None, "just above 'F'");

        // The gap between the two letter ranges is not a range.
        check!(value(b'Z') == None);
        check!(value(b' ') == None);
    }

    /// `parse_decimal_seconds_timestamp` reads seconds with a fractional part
    /// into whole nanoseconds. The fraction is positional -- the first digit
    /// is tenths, not units -- so a scale applied the wrong way round is the
    /// mistake worth catching, and it only shows on a fraction shorter than
    /// nine digits.
    #[test]
    fn decimal_second_timestamps_scale_their_fraction_by_position() {
        let parse = super::parse_decimal_seconds_timestamp;

        check!(parse("0.0") == Some(0));
        check!(parse("1.0") == Some(1_000_000_000));
        check!(
            parse("1.5") == Some(1_500_000_000),
            "one digit is tenths, not units"
        );
        check!(parse("0.5") == Some(500_000_000));
        check!(
            parse("0.05") == Some(50_000_000),
            "the second digit is hundredths"
        );
        check!(
            parse("0.000000001") == Some(1),
            "nine digits reach nanoseconds"
        );

        // Past nine digits the rest is dropped rather than overflowing the
        // scale into zero or below.
        check!(
            parse("0.0000000019") == Some(1),
            "the tenth digit is ignored"
        );

        // Signs, on both sides of zero.
        check!(parse("-1.5") == Some(-1_500_000_000));
        check!(
            parse("+1.5") == Some(1_500_000_000),
            "an explicit plus is allowed"
        );
        check!(parse("-0.0") == Some(0));

        // A missing part on either side of the point is still a number.
        check!(parse("1.") == Some(1_000_000_000), "no fraction");
        check!(parse(".5") == Some(500_000_000), "no whole part");

        // What is not a decimal at all.
        check!(parse("1") == None, "a point is required");
        check!(parse(".") == None, "and digits on one side of it");
        check!(parse("") == None);
        check!(parse("a.b") == None);
        check!(parse("1.5x") == None, "trailing text is not a fraction");
    }

    /// `scalar_literal_len` reports how many bytes at the front of `input`
    /// form a number, so the caller can resume after it. It is a scanner, not
    /// a parser: it must stop at the first byte that cannot extend the
    /// literal, and refuse anything that is not one.
    #[test]
    fn a_scalar_literal_ends_where_the_number_does() {
        let len = super::scalar_literal_len;

        check!(len("1") == Some(1));
        check!(len("1234") == Some(4));
        check!(len("+1") == Some(2), "a leading sign counts");
        check!(len("-1") == Some(2));

        // A fraction may sit on either side of the point.
        check!(len("1.5") == Some(3));
        check!(len(".5") == Some(2), "no whole part is still a number");
        check!(len("1.") == Some(2), "a trailing point ends the literal");
        check!(len("+.5") == Some(3));

        // An exponent takes an optional sign and needs at least one digit.
        check!(len("1e5") == Some(3));
        check!(len("1e+5") == Some(4));
        check!(len("1e-5") == Some(4));
        check!(len("1.5e10") == Some(6));
        check!(len("1E5") == Some(3), "an exponent may be upper case");
        check!(len("1E-5") == Some(4));
        check!(
            len("1e") == None,
            "an exponent with no digits is not a number"
        );
        check!(len("1e+") == None);

        // Nothing that is not a number.
        check!(len("") == None);
        check!(len(".") == None, "a bare point has no digits either side");
        check!(len("+") == None);
        check!(len("abc") == None);

        // The scan stops at the first byte it cannot use, rather than
        // rejecting the whole input.
        check!(len("1abc") == Some(1));
        check!(len("1.5]") == Some(3));
        check!(len("1e5x") == Some(3));
    }

    /// `metadata_label_sets` lists the distinct label sets a tenant has,
    /// filtered by the request's matchers and hiding the labels that are
    /// internal. Replacing its body with an empty list passed the whole suite
    /// before this test, so every part of it is pinned here.
    #[tokio::test]
    async fn metadata_label_sets_are_distinct_filtered_and_stripped() {
        async fn sets(state: &QuerierState, matchers: Vec<String>) -> Vec<Labels> {
            let params = SeriesParams {
                matchers,
                start: None,
                end: None,
                since: None,
            };
            super::metadata_label_sets(state, "t", &params)
                .await
                .expect("readable")
        }
        let mut label_index = LabelIndex::default();
        let labels = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                .collect::<Labels>()
        };
        // Two series for one tenant plus one for another, and a fourth that
        // differs from the first only by an internal label -- so it collapses
        // onto it once that label is hidden.
        label_index.insert_series("t", labels(&[("app", "web"), ("env", "prod")]));
        label_index.insert_series("t", labels(&[("app", "api"), ("env", "prod")]));
        label_index.insert_series(
            "t",
            labels(&[("app", "web"), ("env", "prod"), ("detected_level", "warn")]),
        );
        label_index.insert_series("other", labels(&[("app", "elsewhere")]));

        let dir = tempfile::TempDir::new().expect("temp dir");
        let state = QuerierState::new(dir.path(), label_index, BlockIndex::default());

        // Unfiltered: the two distinct visible sets, with the third collapsed
        // onto the first because its only difference is hidden.
        let all = sets(&state, Vec::new()).await;
        check!(all.len() == 2, "got {all:?}");
        check!(
            all.iter().all(|set| set.get("detected_level").is_none()),
            "the internal label is stripped, not reported"
        );

        // Another tenant's series are not this tenant's.
        check!(
            all.iter()
                .all(|set| set.get("app").map(String::as_str) != Some("elsewhere")),
            "tenant isolation"
        );

        // A matcher narrows the result rather than being ignored.
        let web = sets(&state, vec![r#"{app="web"}"#.to_string()]).await;
        check!(web.len() == 1, "got {web:?}");
        check!(web[0].get("app").map(String::as_str) == Some("web"));

        let none = sets(&state, vec![r#"{app="absent"}"#.to_string()]).await;
        check!(
            none.is_empty(),
            "a matcher that matches nothing returns nothing"
        );
    }

    /// The hot tail is bounded twice over: to the requesting tenant, and to
    /// the requested window at both edges inclusively. Nothing had ever read
    /// it through this endpoint, so a record from another tenant, one before
    /// the window and one after it were all free to be reported, and the two
    /// edges were free to exclude a record sitting exactly on them.
    #[tokio::test]
    async fn metadata_label_sets_bound_the_hot_tail_to_the_window_and_the_tenant() {
        // A hot tail is allowed to answer a range query with a *superset* --
        // the trait says so, because a coarse time index returns whole buckets
        // -- and the caller re-applies the exact bound. This one returns
        // everything, which is the widest superset there is.
        struct CoarseHotTail(Vec<super::WalLogRecord>);
        impl super::LogHotTail for CoarseHotTail {
            fn records(&self) -> Vec<super::WalLogRecord> {
                self.0.clone()
            }
            fn records_in_range(&self, _start_ns: i64, _end_ns: i64) -> Vec<super::WalLogRecord> {
                self.0.clone()
            }
        }

        let record = |tenant: &str, app: &str, timestamp_ns: i64| super::WalLogRecord {
            tenant: tenant.to_string(),
            labels: [("app".to_string(), app.to_string())]
                .into_iter()
                .collect::<Labels>(),
            timestamp_ns,
            line: "line".to_string(),
            structured_metadata: std::collections::BTreeMap::new(),
            position: None,
        };
        let sink = CoarseHotTail(vec![
            record("t", "on_the_start", 100),
            record("t", "inside", 150),
            record("t", "on_the_end", 200),
            record("t", "before", 99),
            record("t", "after", 201),
            record("other", "foreign", 150),
        ]);

        let dir = tempfile::TempDir::new().expect("temp dir");
        let state = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default())
            .with_hot_tail(sink, 0);

        let params = SeriesParams {
            matchers: Vec::new(),
            start: Some(100),
            end: Some(200),
            since: None,
        };
        let sets = super::metadata_label_sets(&state, "t", &params)
            .await
            .expect("readable");

        let mut apps = sets
            .iter()
            .filter_map(|set| set.get("app").map(String::as_str))
            .collect::<Vec<_>>();
        apps.sort_unstable();
        check!(
            apps == vec!["inside", "on_the_end", "on_the_start"],
            "got {apps:?}"
        );
    }

    /// `format_logql_query` returns the canonical spelling of a query, and
    /// what "canonical" means differs by the kind of query it is: a stream
    /// selector round-trips, a scalar expression is folded to its value, and a
    /// vector literal gains an explicit float. Nothing tested any of it --
    /// returning an empty string passed the whole suite.
    #[test]
    fn formatting_a_logql_query_canonicalises_by_kind() {
        let format =
            |query: &str| super::format_logql_query(query).map_err(|error| error.to_string());

        // Stream selectors and pipelines come back as they went in.
        check!(format(r#"{app="web"}"#).unwrap() == r#"{app="web"}"#);
        check!(
            format(r#"{app="web"} |= "boom""#).unwrap() == r#"{app="web"} |= "boom""#,
            "a line filter survives"
        );
        check!(
            format(r#"rate({app="web"}[5m])"#).unwrap() == r#"rate({app="web"}[5m])"#,
            "and a range aggregation"
        );
        check!(format(r#"sum(rate({app="web"}[5m]))"#).unwrap() == r#"sum(rate({app="web"}[5m]))"#);

        // Surrounding whitespace is not part of the query. A stream selector
        // is rebuilt from its parse, so it would come back canonical however
        // it was spaced; the second case is the one that proves trimming,
        // because it is returned as written and only the trim can remove the
        // spaces around it.
        check!(format(r#"  {app="web"}  "#).unwrap() == r#"{app="web"}"#);
        check!(
            format(r#"  sum by (app) (rate({app="web"}[5m])) / 2  "#).unwrap()
                == r#"sum by (app) (rate({app="web"}[5m])) / 2"#,
            "returned as written, less the surrounding space"
        );

        // A comparison gains explicit parentheses, and label_replace loses the
        // spaces between its arguments. Both are reprintings rather than
        // pass-throughs, so neither can be reached by the trim above.
        check!(
            format(r#"count_over_time({app="web"}[5m]) > 1"#).unwrap()
                == r#"(count_over_time({app="web"}[5m]) > 1)"#
        );
        check!(
            format(r#"label_replace(rate({app="web"}[5m]), "a", "b", "c", "d")"#).unwrap()
                == r#"label_replace(rate({app="web"}[5m]),"a","b","c","d")"#
        );

        // A scalar expression is evaluated rather than echoed, which is a
        // different contract from every case above.
        check!(format("1 + 1").unwrap() == "2", "folded, not reprinted");

        // A vector literal is normalised to an explicit float.
        check!(format("vector(1)").unwrap() == "vector(1.000000)");

        // These two reach the fallback that returns a query as written: the
        // dedicated formatter for their shape declines, and only the scalar
        // comparison and the vector-expression parsers below it accept them.
        // Everything above is a reprint, so a pass-through is the signature of
        // having got that far.
        for query in [r#"sum(rate({app="web"}[5m])) > 5"#, "vector(1) + 2"] {
            check!(format(query).unwrap() == query, "{query}");
        }

        // What is not a query at all is an error naming where it gave up,
        // rather than an empty string or the input echoed back.
        let error = format("").unwrap_err();
        check!(error.contains("byte 0"), "got: {error}");
        let error = format("not a query at all").unwrap_err();
        check!(error.contains("byte 0"), "got: {error}");
        let error = format("{").unwrap_err();
        check!(
            error.contains("label name"),
            "a partial selector names what it wanted: {error}"
        );
    }

    /// The shard-range cache answers only when its entry is both fresh and
    /// covers far enough back, and it *evicts* on either failure rather than
    /// leaving the entry to be retried. Both halves matter: a caller that gets
    /// None refetches, and an entry left behind would be rejected again on
    /// every subsequent call while still occupying the map.
    #[test]
    fn a_stale_or_short_shard_range_entry_is_evicted_not_reused() {
        use std::time::{Duration, Instant};

        let key = super::DynamicShardRangesCacheKey {
            tenant: "t".to_string(),
        };
        let ranges = vec![super::TimeRange {
            start_ns: 100,
            end_ns: 200,
        }];

        let seed = |loaded_at: Instant, listed_from_ns: i64| {
            let cache = super::DynamicIndexCache::default();
            cache.shard_ranges.lock().expect("fresh lock").insert(
                key.clone(),
                super::CachedShardRanges {
                    loaded_at,
                    listed_from_ns,
                    ranges: ranges.clone(),
                },
            );
            cache
        };
        let entries =
            |cache: &super::DynamicIndexCache| cache.shard_ranges.lock().expect("fresh lock").len();

        // Fresh, and covering back to 100: a request from 100 or later is served.
        let cache = seed(Instant::now(), 100);
        check!(
            cache.get_shard_ranges(&key, 100) == Some(ranges.clone()),
            "exactly covered"
        );
        check!(
            cache.get_shard_ranges(&key, 150) == Some(ranges.clone()),
            "more than covered"
        );
        check!(entries(&cache) == 1, "a usable entry stays");

        // Asked for earlier than the entry was listed from: not usable, and
        // dropped so the next call refetches rather than re-rejecting.
        let cache = seed(Instant::now(), 100);
        check!(
            cache.get_shard_ranges(&key, 99) == None,
            "one nanosecond short"
        );
        check!(entries(&cache) == 0, "and evicted");

        // Older than the five-second default TTL.
        let stale = Instant::now()
            .checked_sub(Duration::from_mins(1))
            .expect("an instant a minute ago");
        let cache = seed(stale, 100);
        check!(cache.get_shard_ranges(&key, 100) == None, "expired");
        check!(entries(&cache) == 0, "and evicted");

        // A key that was never cached is simply absent, and nothing is
        // inserted by asking for it.
        let cache = super::DynamicIndexCache::default();
        check!(cache.get_shard_ranges(&key, 100) == None);
        check!(entries(&cache) == 0);
    }

    /// `post_query_params` merges a URL query with a form body. It has a
    /// near-twin, `post_query_params_body_first`, which differs only in which
    /// side leads the result -- so every case here asserts the order, not just
    /// the contents. A test that checked membership alone would pass against
    /// either function and distinguish neither.
    #[test]
    fn a_posted_query_puts_the_url_first_and_the_body_second() {
        let merge = |raw: Option<&str>, body: &str| {
            super::post_query_params(raw, &Bytes::from(body.to_owned())).expect("valid body")
        };
        let body_first = |raw: Option<&str>, body: &str| {
            super::post_query_params_body_first(raw, &Bytes::from(body.to_owned()))
                .expect("valid body")
        };

        // Both sides present: the order is the whole difference between the
        // two functions.
        check!(merge(Some("a=1"), "b=2") == "a=1&b=2");
        check!(body_first(Some("a=1"), "b=2") == "b=2&a=1");

        // One side only, where the two agree.
        check!(merge(Some("a=1"), "") == "a=1");
        check!(merge(None, "b=2") == "b=2");
        check!(merge(None, "") == "");

        // An empty URL query is treated as absent rather than concatenated,
        // which would otherwise leave a leading separator.
        check!(merge(Some(""), "b=2") == "b=2");
        check!(merge(Some(""), "") == "");
        check!(body_first(Some(""), "b=2") == "b=2");
    }

    /// The rules filters read a `Prometheus`-shaped query. Each recognised key
    /// is guarded on its value, so a key carrying something unexpected leaves
    /// the filter unset rather than setting it to a default.
    #[test]
    fn rules_filters_take_only_the_values_they_recognise() {
        use super::PrometheusRulesFilters as Filters;
        let parse = |q: &str| Filters::parse(Some(q)).expect("valid query");

        // `type` maps two spellings and rejects the rest.
        check!(parse("type=alert").rule_kind == Some("alerting"));
        check!(parse("type=record").rule_kind == Some("recording"));
        check!(
            parse("type=other").rule_kind == None,
            "an unknown type sets nothing"
        );
        check!(parse("type=").rule_kind == None);
        check!(
            parse("type=alerting").rule_kind == None,
            "the output spelling is not the input"
        );

        // `exclude_alerts` is only true for the exact string.
        check!(parse("exclude_alerts=true").exclude_alerts);
        check!(!parse("exclude_alerts=false").exclude_alerts);
        check!(
            !parse("exclude_alerts=1").exclude_alerts,
            "only `true` counts"
        );
        check!(
            !parse("exclude_alerts=TRUE").exclude_alerts,
            "case-sensitively"
        );
        check!(!Filters::parse(None).expect("no query").exclude_alerts);

        // The repeated keys accept both spellings and collect rather than
        // replace, and an empty value is skipped rather than collected.
        let names = parse("rule_name=a&rule_name[]=b&rule_name=").rule_names;
        check!(names.len() == 2, "got {names:?}");
        check!(names.contains("a") && names.contains("b"));

        let groups = parse("rule_group=g1&rule_group[]=g2").rule_groups;
        check!(groups.len() == 2);

        // No query at all is a default set of filters, not an error.
        let empty = Filters::parse(None).expect("no query");
        check!(empty.rule_kind == None);
        check!(empty.rule_names.is_empty());
    }

    /// The comparison operators are a six-entry table, and every strict one
    /// has a non-strict twin one character longer. Checking them entry by
    /// entry is what separates the pairs; sampling would not.
    #[test]
    fn every_metric_comparison_operator_maps_to_its_own_variant() {
        use super::{ComparisonOp, parse_metric_comparison_operator as parse};

        check!(parse("==") == Some(ComparisonOp::Equal));
        check!(parse("!=") == Some(ComparisonOp::NotEqual));
        check!(parse(">") == Some(ComparisonOp::Greater));
        check!(parse(">=") == Some(ComparisonOp::GreaterEqual));
        check!(parse("<") == Some(ComparisonOp::Less));
        check!(parse("<=") == Some(ComparisonOp::LessEqual));

        // Nothing else is an operator, including the near-misses.
        check!(parse("") == None);
        check!(parse("=") == None, "a single equals is not a comparison");
        check!(parse("=>") == None);
        check!(parse("=<") == None);
        check!(parse("<>") == None);
        check!(parse("===") == None);
        check!(parse(">>") == None);
    }

    /// `parse_vector_group_modifier` returns the modifier it read and how many
    /// bytes it consumed. The length is the half that matters: a caller
    /// resumes from it, so an off-by-one there re-reads a character or skips
    /// one, and the returned text alone would look correct either way.
    #[test]
    fn a_vector_group_modifier_reports_what_it_consumed() {
        let parse = super::parse_vector_group_modifier;

        // Bare, with the length being the whole modifier.
        check!(parse("group_left", 0) == Some(("group_left".to_string(), 10)));
        check!(parse("group_right", 0) == Some(("group_right".to_string(), 11)));

        // With labels, the length covers the parentheses too.
        check!(parse("group_left(a)", 0) == Some(("group_left (a)".to_string(), 13)));
        check!(parse("group_right(a,b)", 0) == Some(("group_right (a,b)".to_string(), 16)));

        // Empty parentheses are consumed but add no labels.
        check!(parse("group_left()", 0) == Some(("group_left".to_string(), 12)));

        // The length is relative to the whole query, not to the slice read.
        check!(parse("x group_left", 2) == Some(("group_left".to_string(), 12)));
        check!(parse("x group_left(a)", 2) == Some(("group_left (a)".to_string(), 15)));

        // Trailing input is left for the caller rather than swallowed.
        check!(parse("group_left(a) foo", 0) == Some(("group_left (a)".to_string(), 13)));

        // An unclosed parenthesis is not a modifier at all.
        check!(parse("group_left(a", 0) == None);
        check!(parse("nothing", 0) == None);
        check!(parse("", 0) == None);
    }

    /// `MetricValue` is a rational scaled by a fixed decimal factor, so a
    /// float arriving from a metric has to survive the round trip through that
    /// scale, and the values it cannot represent have to be refused rather
    /// than rounded into something plausible.
    #[test]
    fn metric_values_round_trip_through_their_decimal_scale() {
        use super::MetricValue;

        let round_trip =
            |value: f64| MetricValue::from_f64(value).and_then(super::MetricValue::to_f64);

        check!(round_trip(0.0) == Some(0.0));
        check!(round_trip(1.0) == Some(1.0));
        check!(round_trip(-1.0) == Some(-1.0));
        check!(round_trip(0.5) == Some(0.5));
        check!(round_trip(-2.25) == Some(-2.25));
        check!(round_trip(1234.5) == Some(1234.5));

        // The scale is a billion, so a nanosecond-sized fraction survives and
        // anything finer rounds to the nearest step rather than to zero.
        check!(round_trip(0.000_000_001) == Some(0.000_000_001));
        check!(
            round_trip(0.000_000_000_4) == Some(0.0),
            "below half a step rounds down"
        );
        check!(
            round_trip(0.000_000_000_6) == Some(0.000_000_001),
            "above half rounds up"
        );

        // Values that are not numbers cannot be represented at all.
        check!(MetricValue::from_f64(f64::NAN) == None);
        check!(MetricValue::from_f64(f64::INFINITY) == None);
        check!(MetricValue::from_f64(f64::NEG_INFINITY) == None);
    }

    /// `MetricValue::modulo` refuses a zero divisor rather than producing a
    /// NaN, which is the whole reason it is not just `%`.
    #[test]
    fn metric_modulo_refuses_a_zero_divisor() {
        use super::MetricValue;

        let modulo = |a: f64, b: f64| {
            MetricValue::from_f64(a)?
                .modulo(MetricValue::from_f64(b)?)
                .and_then(super::MetricValue::to_f64)
        };

        check!(modulo(7.0, 3.0) == Some(1.0));
        check!(modulo(7.5, 2.5) == Some(0.0));
        check!(
            modulo(-7.0, 3.0) == Some(-1.0),
            "the sign follows the dividend"
        );
        check!(
            modulo(3.0, 7.0) == Some(3.0),
            "a smaller dividend is itself"
        );
        check!(modulo(1.0, 0.0) == None, "a zero divisor has no answer");
        check!(modulo(0.0, 3.0) == Some(0.0), "but a zero dividend does");
    }

    /// `has_samples` gates every aggregate that would otherwise divide by a
    /// count of zero, so it must be false at zero and true at one.
    #[test]
    fn a_sample_state_has_samples_from_the_first_one() {
        let mut state = super::MetricSampleState::default();
        check!(!state.has_samples(), "an empty state has none");

        state.count = 1;
        check!(state.has_samples(), "one sample is enough");
        state.count = 100;
        check!(state.has_samples());
    }

    /// Recording keeps the earliest sample and the latest, and a later record
    /// at a timestamp already held changes neither. The four below arrive out
    /// of order and revisit both ends: without the revisits, the guards could
    /// take the last writer at each end instead of the first.
    #[test]
    fn recording_samples_keeps_the_earliest_and_the_latest() {
        let value = |numerator: i128| super::MetricValue {
            numerator,
            denominator: 1,
        };
        let mut state = super::MetricSampleState::default();

        state.record(10, value(1));
        state.record(5, value(2));
        // Neither of these displaces an end: one repeats the latest timestamp,
        // the other the earliest.
        state.record(10, value(3));
        state.record(5, value(4));

        check!(state.count == 4);
        check!(
            state.first == Some((5, value(2))),
            "the earliest timestamp, from the first record that reached it"
        );
        check!(
            state.last == Some((10, value(1))),
            "the latest timestamp, from the first record that reached it"
        );
    }

    /// A scalar renders its sign from the numerator alone, and a decimal that
    /// does not terminate stops at nine digits. Zero is not negative, which is
    /// what separates `< 0` from `<= 0`; a negative that is not zero separates
    /// it from `== 0`; and a repeating decimal is the only input that reaches
    /// the digit cap at all.
    #[test]
    fn a_scalar_sample_formats_its_sign_and_stops_at_nine_decimals() {
        let format = |numerator: i128, denominator: u128| {
            super::ScalarSample::new(numerator, denominator).format()
        };

        check!(format(0, 1) == "0", "zero carries no sign");
        check!(format(7, 1) == "7");
        check!(format(-7, 1) == "-7");
        check!(format(3, 2) == "1.5");
        check!(format(-3, 2) == "-1.5");
        check!(format(1, 8) == "0.125");

        // Truncated at nine digits, not rounded and not run on.
        check!(format(1, 3) == "0.333333333");
        check!(format(-2, 3) == "-0.666666666");
    }

    /// Merging two partial sample states keeps the smaller minimum, the larger
    /// maximum, the earliest first and the latest last, taking each from
    /// whichever side holds it. A tie on the timestamp keeps the side already
    /// held -- the only thing that separates `<` from `<=` at either end.
    #[test]
    fn merging_sample_states_keeps_the_extremes_and_the_ends() {
        let value = |numerator: i128| super::MetricValue {
            numerator,
            denominator: 1,
        };

        let mut left = super::MetricSampleState {
            count: 1,
            min: Some(value(5)),
            max: Some(value(5)),
            first: Some((10, value(1))),
            last: Some((10, value(1))),
            ..Default::default()
        };
        // Every field of the incoming state wins: a lower minimum, a higher
        // maximum, an earlier first and a later last.
        left.merge(super::MetricSampleState {
            count: 1,
            min: Some(value(3)),
            max: Some(value(9)),
            first: Some((5, value(2))),
            last: Some((20, value(3))),
            ..Default::default()
        });

        check!(left.count == 2);
        check!(left.min == Some(value(3)), "the smaller minimum wins");
        check!(left.max == Some(value(9)), "the larger maximum wins");
        check!(left.first == Some((5, value(2))), "the earlier first wins");
        check!(left.last == Some((20, value(3))), "the later last wins");

        // Now the other way round, so neither side is simply preferred.
        let mut right = super::MetricSampleState {
            count: 1,
            min: Some(value(3)),
            max: Some(value(9)),
            first: Some((5, value(2))),
            last: Some((20, value(3))),
            ..Default::default()
        };
        right.merge(super::MetricSampleState {
            count: 1,
            min: Some(value(5)),
            max: Some(value(5)),
            first: Some((10, value(1))),
            last: Some((10, value(1))),
            ..Default::default()
        });
        check!(right.min == Some(value(3)), "the held minimum survives");
        check!(right.max == Some(value(9)), "the held maximum survives");
        check!(
            right.first == Some((5, value(2))),
            "the held first survives"
        );
        check!(right.last == Some((20, value(3))), "the held last survives");

        // Matching timestamps on both sides: the value already held stays.
        let mut tied = super::MetricSampleState {
            count: 1,
            first: Some((10, value(1))),
            last: Some((10, value(1))),
            ..Default::default()
        };
        tied.merge(super::MetricSampleState {
            count: 1,
            first: Some((10, value(7))),
            last: Some((10, value(7))),
            ..Default::default()
        });
        check!(
            tied.first == Some((10, value(1))),
            "a tie keeps the first already held"
        );
        check!(
            tied.last == Some((10, value(1))),
            "a tie keeps the last already held"
        );
    }

    /// Every rules filter that takes a value ignores an empty one. Without
    /// that guard `time=`, `group_limit=` and `match=` are handed the empty
    /// string to parse and the whole request fails, while `rule_group=`,
    /// `file=` and `group_next_token=` quietly filter on "" and match nothing.
    /// A query naming all of them with no values is indistinguishable from no
    /// query at all.
    #[test]
    fn empty_prometheus_rules_filter_values_are_ignored() {
        let filters = super::PrometheusRulesFilters::parse(Some(
            "time=&rule_name=&rule_group=&file=&group_limit=&group_next_token=&match=",
        ))
        .expect("empty values are ignored, not rejected");

        check!(filters == super::PrometheusRulesFilters::default());
    }

    /// Two sightings of the same field can disagree about its type, and the
    /// merge picks what still describes both. The arms are ordered, so
    /// deleting one does not fail -- it falls through to the catch-all and
    /// quietly widens to a string. Only a pair that a *later* arm would also
    /// match shows the difference, so the whole six-by-six table is here.
    #[test]
    fn detected_field_types_merge_to_what_still_describes_both() {
        use super::DetectedFieldType as Type;

        let cases = [
            (Type::Boolean, Type::Boolean, Type::Boolean),
            (Type::Boolean, Type::Int, Type::String),
            (Type::Boolean, Type::Float, Type::Float),
            (Type::Boolean, Type::Duration, Type::String),
            (Type::Boolean, Type::Bytes, Type::String),
            (Type::Boolean, Type::String, Type::String),
            (Type::Int, Type::Boolean, Type::String),
            (Type::Int, Type::Int, Type::Int),
            (Type::Int, Type::Float, Type::Float),
            (Type::Int, Type::Duration, Type::String),
            (Type::Int, Type::Bytes, Type::String),
            (Type::Int, Type::String, Type::String),
            (Type::Float, Type::Boolean, Type::Float),
            (Type::Float, Type::Int, Type::Float),
            (Type::Float, Type::Float, Type::Float),
            (Type::Float, Type::Duration, Type::Float),
            (Type::Float, Type::Bytes, Type::Float),
            (Type::Float, Type::String, Type::String),
            (Type::Duration, Type::Boolean, Type::String),
            (Type::Duration, Type::Int, Type::String),
            (Type::Duration, Type::Float, Type::Float),
            (Type::Duration, Type::Duration, Type::Duration),
            (Type::Duration, Type::Bytes, Type::String),
            (Type::Duration, Type::String, Type::String),
            (Type::Bytes, Type::Boolean, Type::String),
            (Type::Bytes, Type::Int, Type::String),
            (Type::Bytes, Type::Float, Type::Float),
            (Type::Bytes, Type::Duration, Type::String),
            (Type::Bytes, Type::Bytes, Type::Bytes),
            (Type::Bytes, Type::String, Type::String),
            (Type::String, Type::Boolean, Type::String),
            (Type::String, Type::Int, Type::String),
            (Type::String, Type::Float, Type::String),
            (Type::String, Type::Duration, Type::String),
            (Type::String, Type::Bytes, Type::String),
            (Type::String, Type::String, Type::String),
        ];

        for (left, right, want) in cases {
            check!(left.merge(right) == want, "{left:?} with {right:?}");
        }
    }

    /// The detected-labels parser is first-wins on every parameter, and none
    /// of them has a default that a repeat could be mistaken for. A guard
    /// stuck open makes the last value win; a guard stuck shut drops the
    /// parameter entirely and the default takes over -- so each is repeated
    /// with a different value, and `since` uses two spans that differ from the
    /// one-hour default as well as from each other.
    #[test]
    fn a_repeated_detected_labels_parameter_keeps_the_first_value() {
        let parse = |q: &str| super::parse_detected_labels_params(Some(q)).expect("a valid query");

        let params = parse(
            "query={a=\"b\"}&query={c=\"d\"}&start=100&start=200&end=900&end=800&limit=5&limit=9",
        );
        check!(params.query.as_deref() == Some("{a=\"b\"}"));
        check!(params.start == 100);
        check!(params.end == 900);
        check!(params.limit == 5);

        // `since` is read only when `start` is absent, and it sets the span
        // back from `end`. Two hours, not thirty minutes and not the one-hour
        // default.
        let params = parse("end=10000000000000&since=2h&since=30m");
        check!(params.end - params.start == 7_200_000_000_000);
    }

    /// The main query parser carries the same first-wins contract, across all
    /// ten of its parameters. None of them has a default, so a repeat is the
    /// only way to tell the guard from its absence.
    #[test]
    fn a_repeated_log_query_parameter_keeps_the_first_value() {
        let parse = |q: &str| super::parse_query_params(Some(q)).expect("valid query");

        check!(parse("query=a&query=b").query == "a");
        // A LogQL selector contains `=` itself, so the split has to take the
        // first one: taking the last would cut the value in half and leave the
        // remainder attached to the key.
        check!(
            parse(r#"query={app="web"}"#).query == r#"{app="web"}"#,
            "the value keeps its own `=`"
        );
        check!(parse("query=a&time=100&time=200").time == Some(100));
        check!(parse("query=a&start=100&start=200").start == Some(100));
        check!(parse("query=a&end=500&end=900").end == Some(500));
        check!(parse("query=a&limit=5&limit=9").limit == Some(5));
        check!(
            parse("query=a&direction=forward&direction=backward").direction
                == Some("forward".to_string())
        );
        // The four duration parameters, which the cases above never repeat.
        // Two hours against thirty minutes, so neither reading is the other.
        check!(parse("query=a&since=2h&since=30m").since == Some(7_200_000_000_000));
        check!(parse("query=a&step=2h&step=30m").step == Some(7_200_000_000_000));
        check!(parse("query=a&interval=2h&interval=30m").interval == Some(7_200_000_000_000));
        // `delay_for` reads a bare number as seconds.
        check!(parse("query=a&delay_for=1&delay_for=2").delay_for == Some(1_000_000_000));

        // Absent parameters stay absent rather than acquiring a value.
        let bare = parse("query=a");
        check!(bare.since == None);
        check!(bare.step == None);
        check!(bare.interval == None);
        check!(bare.delay_for == None);
        check!(bare.time == None);
        check!(bare.start == None);
        check!(bare.end == None);
        check!(bare.limit == None);
        check!(bare.direction == None);

        // Splitting is key-aware: an `&` only ends a parameter when a known
        // key and its `=` follow. That is what lets a LogQL query contain an
        // `&` without being truncated at it.
        check!(
            parse("query=a&direction").query == "a&direction",
            "a bare `&` is part of the value"
        );
        check!(parse("query=a&direction").direction == None);
        check!(
            parse("query=a&b&limit=5").query == "a&b",
            "and so is one followed by an unknown key"
        );
        check!(
            parse("query=a&b&limit=5").limit == Some(5),
            "the known key still splits"
        );

        // A query parameter is still required.
        check!(super::parse_query_params(Some("limit=5")).is_err());
        check!(super::parse_query_params(None).is_err());
    }

    /// A repeated query parameter keeps its first value and ignores the rest.
    ///
    /// Each arm of the parse loop is guarded on the field still being unset, so
    /// a second occurrence falls through to the catch-all and is dropped. With
    /// the guard gone the last occurrence would win instead, which no test
    /// passing a well-formed query once can tell apart -- the values have to
    /// differ and the query has to repeat.
    #[test]
    fn a_repeated_volume_parameter_keeps_the_first_value() {
        let parse = |q: &str| super::parse_volume_params(Some(q)).expect("valid query");

        check!(parse("query=a&query=b").query == "a");
        check!(parse("query=a&limit=5&limit=9").limit == 5);
        check!(parse("query=a&start=100&start=200").start == 100);
        check!(parse("query=a&end=500&end=900").end == 500);
        check!(parse("query=a&step=5s&step=9s").step == parse("query=a&step=5s").step);
        check!(
            parse("query=a&targetLabels=x&targetLabels=y").target_labels
                == Some(vec!["x".to_string()])
        );
        check!(matches!(
            parse("query=a&aggregateBy=labels&aggregateBy=series").aggregate_by,
            super::VolumeAggregateBy::Labels
        ));

        // The defaults still apply when a parameter is absent entirely, which
        // is a different thing from being repeated.
        check!(parse("query=a").limit == 100);
        check!(matches!(
            parse("query=a").aggregate_by,
            super::VolumeAggregateBy::Series
        ));
        check!(parse("query=a").target_labels == None);

        // An empty label in the list is dropped rather than kept as "".
        check!(
            parse("query=a&targetLabels=x,,y").target_labels
                == Some(vec!["x".to_string(), "y".to_string()])
        );

        // A query with no `query` at all is an error, not a default.
        check!(super::parse_volume_params(Some("limit=5")).is_err());
        check!(super::parse_volume_params(None).is_err());
        // An unknown aggregation is rejected rather than falling back.
        check!(super::parse_volume_params(Some("query=a&aggregateBy=nonsense")).is_err());
    }

    /// The detected-fields parser carries the same first-wins contract.
    #[test]
    fn a_repeated_detected_fields_parameter_keeps_the_first_value() {
        let parse = |q: &str| super::parse_detected_fields_params(Some(q)).expect("valid query");

        check!(parse("query=a&query=b").query == "a");
        check!(parse("query=a&limit=5&limit=9").limit == 5);
        check!(parse("query=a&start=100&start=200").start == 100);
        check!(parse("query=a&end=500&end=900").end == 500);
        check!(parse("query=a&line_limit=7&line_limit=11").line_limit == 7);

        // `field_limit` is an alias for `limit`, guarded on the same field, so
        // first-wins spans the pair rather than each name separately.
        check!(
            parse("query=a&field_limit=9").limit == 9,
            "the alias sets limit"
        );
        check!(
            parse("query=a&limit=5&field_limit=9").limit == 5,
            "limit first"
        );
        check!(
            parse("query=a&field_limit=9&limit=5").limit == 9,
            "alias first"
        );

        // Defaults apply when absent, which is distinct from being repeated.
        check!(parse("query=a").limit == 1000);
        check!(parse("query=a").line_limit == 100);

        check!(super::parse_detected_fields_params(Some("limit=5")).is_err());
        check!(super::parse_detected_fields_params(None).is_err());
    }

    /// `ScalarSample::compare` orders two rationals by cross-multiplication,
    /// so the fractions below are chosen not to be decided by their numerators
    /// alone: 1/2 against 2/3 orders one way and 2/3 against 1/2 the other,
    /// while 1/2 and 2/4 are equal without being identical. A comparison that
    /// forgot to cross-multiply would still get many pairs right.
    #[test]
    fn scalar_samples_compare_as_rationals() {
        use super::{ScalarComparisonOp as Op, ScalarSample};

        let cmp = |n1: i128, d1: u128, op, n2: i128, d2: u128| {
            ScalarSample::new(n1, d1).compare(op, ScalarSample::new(n2, d2))
        };

        // 1/2 < 2/3, which no comparison of numerators alone would decide.
        check!(cmp(1, 2, Op::Less, 2, 3) == Some(true));
        check!(cmp(1, 2, Op::Greater, 2, 3) == Some(false));
        check!(cmp(2, 3, Op::Greater, 1, 2) == Some(true));

        // Equal values with different representations.
        check!(cmp(1, 2, Op::Equal, 2, 4) == Some(true));
        check!(cmp(1, 2, Op::NotEqual, 2, 4) == Some(false));
        check!(
            cmp(1, 2, Op::LessOrEqual, 2, 4) == Some(true),
            "equal satisfies <="
        );
        check!(cmp(1, 2, Op::GreaterOrEqual, 2, 4) == Some(true), "and >=");
        check!(cmp(1, 2, Op::Less, 2, 4) == Some(false), "but not <");
        check!(cmp(1, 2, Op::Greater, 2, 4) == Some(false), "nor >");

        // Each strict operator against its non-strict twin, on a pair that is
        // not equal, so the two cannot be confused for one another.
        check!(cmp(1, 3, Op::Less, 1, 2) == Some(true));
        check!(cmp(1, 3, Op::LessOrEqual, 1, 2) == Some(true));
        check!(cmp(1, 2, Op::Greater, 1, 3) == Some(true));
        check!(cmp(1, 2, Op::GreaterOrEqual, 1, 3) == Some(true));

        // Signs, including a negative on either side of zero.
        check!(cmp(-1, 2, Op::Less, 1, 2) == Some(true));
        check!(
            cmp(-1, 2, Op::Less, -1, 3) == Some(true),
            "-1/2 is below -1/3"
        );
        check!(cmp(-1, 2, Op::Equal, -2, 4) == Some(true));
        check!(
            cmp(0, 1, Op::Equal, 0, 5) == Some(true),
            "zero is zero at any scale"
        );

        // A product that cannot fit answers nothing rather than wrapping.
        check!(cmp(i128::MAX, 1, Op::Greater, 1, 2) == None);
    }

    /// `prometheus_duration_unit` maps a unit to its ordinal, its bit, and how
    /// many nanoseconds it is worth. The ordinals and bits are checked as for
    /// `detected_duration_unit`; the nanoseconds are checked against each
    /// other rather than restated, because a wrong power of ten in a column of
    /// long literals is invisible read straight and obvious as a ratio.
    #[test]
    fn duration_units_are_worth_what_they_should_relative_to_each_other() {
        let ns = |name: &str| {
            let (_, _, nanos) = super::prometheus_duration_unit(name).expect("known unit");
            nanos
        };

        check!(ns("ns") == 1, "the base unit");
        check!(ns("us") == 1_000 * ns("ns"));
        check!(ns("ms") == 1_000 * ns("us"));
        check!(ns("s") == 1_000 * ns("ms"));
        check!(ns("m") == 60 * ns("s"));
        check!(ns("h") == 60 * ns("m"));
        check!(ns("d") == 24 * ns("h"));
        check!(ns("w") == 7 * ns("d"));
        check!(
            ns("y") == 365 * ns("d"),
            "a year here is 365 days, not 52 weeks"
        );

        // The ordinal and bit columns carry the same contract as the detected
        // table, so they get the same check.
        for (name, ordinal) in [
            ("y", 0_u8),
            ("w", 1),
            ("d", 2),
            ("h", 3),
            ("m", 4),
            ("s", 5),
            ("ms", 6),
            ("us", 7),
            ("ns", 8),
        ] {
            let (got, bit, _) = super::prometheus_duration_unit(name).expect("known unit");
            check!(got == ordinal, "{name} ordinal");
            check!(bit == 1_u16 << ordinal, "{name} bit");
        }

        check!(super::prometheus_duration_unit("") == None);
        check!(super::prometheus_duration_unit("mo") == None);
        check!(
            super::prometheus_duration_unit("S") == None,
            "case-sensitive"
        );
    }

    /// `detected_duration_unit` maps a unit to its ordinal and its bit. Both
    /// come from the same table, and a table is exactly where an off-by-one
    /// goes unnoticed, so every entry is checked rather than sampled -- and
    /// the bit is checked against the ordinal it is meant to shadow.
    #[test]
    fn every_duration_unit_maps_to_its_ordinal_and_bit() {
        let unit = super::detected_duration_unit;

        for (name, ordinal) in [
            ("y", 0_u8),
            ("w", 1),
            ("d", 2),
            ("h", 3),
            ("m", 4),
            ("s", 5),
            ("ms", 6),
            ("us", 7),
            ("ns", 8),
        ] {
            let expected = (ordinal, 1_u16 << ordinal);
            check!(unit(name) == Some(expected), "{name}");
        }

        // The bits are distinct, which is what makes them usable as a set.
        let mut seen = 0_u16;
        for name in ["y", "w", "d", "h", "m", "s", "ms", "us", "ns"] {
            let (_, bit) = unit(name).expect("known unit");
            check!(seen & bit == 0, "{name} reuses a bit");
            seen |= bit;
        }

        check!(unit("") == None);
        check!(unit("Y") == None, "the match is case-sensitive");
        check!(unit("mo") == None, "months are not a unit here");
        check!(unit("sec") == None);
    }

    /// `parse_logfmt_pairs` walks a logfmt line byte by byte: whitespace
    /// separates pairs, `=` separates a key from its value, and a quoted value
    /// may contain both. Every case below fixes one decision that boundary
    /// takes, since a parser that is off by one still returns pairs -- just
    /// the wrong ones.
    #[test]
    fn logfmt_pairs_split_on_unquoted_whitespace() {
        let parse = super::parse_logfmt_pairs;
        let pair = |k: &str, v: &str| (k.to_string(), v.to_string());

        check!(parse("a=1") == vec![pair("a", "1")]);
        // An unquoted value carrying letters, so a transformation of the slice
        // is visible: digits alone survive most of them unchanged.
        check!(parse("level=warn") == vec![pair("level", "warn")]);
        check!(parse("msg=hello level=warn") == vec![pair("msg", "hello"), pair("level", "warn")]);
        check!(parse("a=1 b=2") == vec![pair("a", "1"), pair("b", "2")]);
        check!(
            parse("  a=1   b=2  ") == vec![pair("a", "1"), pair("b", "2")],
            "runs of whitespace are separators, not content"
        );
        check!(parse("") == vec![], "an empty line has no pairs");
        check!(parse("   ") == vec![], "nor does whitespace alone");

        // A key with nothing after the `=` is a pair with an empty value,
        // which is not the same as the key being absent.
        check!(parse("a=") == vec![pair("a", "")]);
        check!(parse("a= b=2") == vec![pair("a", ""), pair("b", "2")]);

        // A bare token is not a pair and must not swallow the next one.
        check!(parse("bare a=1") == vec![pair("a", "1")]);
        check!(parse("a=1 bare") == vec![pair("a", "1")]);
        check!(parse("bare") == vec![]);

        // A leading `=` has an empty key, which is skipped rather than
        // recorded under an empty name.
        check!(parse("=1 a=2") == vec![pair("a", "2")]);

        // Quoted values hold what unquoted ones cannot.
        check!(
            parse(r#"a="x y""#) == vec![pair("a", "x y")],
            "whitespace inside quotes"
        );
        check!(parse(r#"a="x y" b=2"#) == vec![pair("a", "x y"), pair("b", "2")]);
        // An escape inside a quoted value. Every other quoted case here is
        // escape-free, so the two steps the escape branch takes -- over the
        // backslash and over what it protects -- were never taken at all.
        check!(
            parse(r#"a="x \"y\" z""#) == vec![pair("a", r#"x "y" z"#)],
            "an escaped quote is content, not the end of the value"
        );
        check!(
            parse(r#"a="x\\y""#) == vec![pair("a", r"x\y")],
            "an escaped backslash is one backslash"
        );
        // A backslash with nothing after it is not an escape: there is no
        // second byte to step over.
        check!(parse("a=\"x\\") == vec![pair("a", "x\\")]);

        check!(
            parse(r#"a="""#) == vec![pair("a", "")],
            "an empty quoted value"
        );
        check!(
            parse(r#"a="x\"y" b=2"#) == vec![pair("a", "x\"y"), pair("b", "2")],
            "an escaped quote does not end the value"
        );
        check!(
            parse(r#"a="x\\y""#) == vec![pair("a", "x\\y")],
            "an escaped backslash is one backslash"
        );

        // An unterminated quote runs to the end of the line rather than
        // dropping the pair.
        check!(parse(r#"a="x y"#) == vec![pair("a", "x y")]);
        // A trailing backslash has nothing to escape and is taken literally.
        check!(parse(r#"a="x\"#) == vec![pair("a", "x\\")]);
    }

    /// Sorting a Loki vector result orders it by sample value, and touches
    /// nothing else: a matrix carries the same shape but must come back in the
    /// order it arrived. Nothing had called this at all, so returning without
    /// doing anything -- or sorting exactly the results it should not -- both
    /// passed.
    #[test]
    fn sorting_a_loki_vector_result_orders_only_a_vector() {
        let sample = |value: &str| serde_json::json!({"metric": {"n": value}, "value": [0, value]});
        let order = |value: &serde_json::Value| {
            value
                .pointer("/data/result")
                .and_then(serde_json::Value::as_array)
                .expect("a result array")
                .iter()
                .map(|entry| {
                    entry
                        .pointer("/metric/n")
                        .and_then(serde_json::Value::as_str)
                        .expect("a name")
                        .to_string()
                })
                .collect::<Vec<_>>()
        };

        let mut vector = serde_json::json!({
            "data": { "resultType": "vector", "result": [sample("3"), sample("1"), sample("2")] }
        });
        super::sort_loki_vector_result(&mut vector, false);
        check!(order(&vector) == vec!["1", "2", "3"], "ascending");

        super::sort_loki_vector_result(&mut vector, true);
        check!(
            order(&vector) == vec!["3", "2", "1"],
            "descending reverses it"
        );

        // Same shape, different result type: left exactly as it came.
        let mut matrix = serde_json::json!({
            "data": { "resultType": "matrix", "result": [sample("3"), sample("1")] }
        });
        super::sort_loki_vector_result(&mut matrix, false);
        check!(
            order(&matrix) == vec!["3", "1"],
            "a matrix is not reordered"
        );
    }

    /// `ingest_tenant` returns a present non-empty `X-Scope-OrgID` verbatim,
    /// but falls back to `"unknown"` when the header is missing or empty.
    #[test]
    fn ingest_tenant_reads_header_or_falls_back() {
        let mut present = HeaderMap::new();
        present.insert("X-Scope-OrgID", "acme".parse().unwrap());
        assert_eq!(ingest_tenant(&present), "acme");

        let missing = HeaderMap::new();
        assert_eq!(ingest_tenant(&missing), "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("X-Scope-OrgID", "".parse().unwrap());
        assert_eq!(ingest_tenant(&empty), "unknown");
    }

    #[tokio::test]
    async fn unavailable_query_authorizer_fails_closed() {
        let result = UnavailableQueryAuthorizer.check("tenant-a").await;

        assert2::assert!(matches!(
            result,
            Err(QueryAuthorizationError::Unavailable { tenant, .. }) if tenant == "tenant-a"
        ));
    }

    #[test]
    fn service_readiness_requires_wal_and_authorization() {
        assert2::assert!(ServiceReadiness::ready().is_ready());

        let readiness = ServiceReadiness::deferred_querier();
        assert2::assert!(!readiness.is_ready());
        readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
        assert2::assert!(!readiness.is_ready());
        readiness.wal_connected.store(false, AtomicOrdering::SeqCst);
        readiness
            .authorization_connected
            .store(true, AtomicOrdering::SeqCst);
        assert2::assert!(!readiness.is_ready());
        readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
        assert2::assert!(readiness.is_ready());
    }

    #[derive(Clone)]
    struct RecordingObjectStore {
        inner: Arc<object_store::memory::InMemory>,
        put_paths: Arc<Mutex<Vec<String>>>,
        get_paths: Arc<Mutex<Vec<String>>>,
        list_prefixes: Arc<Mutex<Vec<String>>>,
        list_offsets: Arc<Mutex<Vec<String>>>,
        get_delay: Duration,
        active_gets: Arc<std::sync::atomic::AtomicUsize>,
        max_active_gets: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RecordingObjectStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(object_store::memory::InMemory::new()),
                put_paths: Arc::new(Mutex::new(Vec::new())),
                get_paths: Arc::new(Mutex::new(Vec::new())),
                list_prefixes: Arc::new(Mutex::new(Vec::new())),
                list_offsets: Arc::new(Mutex::new(Vec::new())),
                get_delay: Duration::ZERO,
                active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_active_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn with_get_delay(mut self, get_delay: Duration) -> Self {
            self.get_delay = get_delay;
            self
        }

        fn clear_recorded_paths(&self) {
            self.put_paths.lock().unwrap().clear();
            self.get_paths.lock().unwrap().clear();
            self.list_prefixes.lock().unwrap().clear();
            self.list_offsets.lock().unwrap().clear();
        }

        fn clear_put_paths(&self) {
            self.put_paths.lock().unwrap().clear();
        }

        fn put_paths(&self) -> Vec<String> {
            self.put_paths.lock().unwrap().clone()
        }

        fn get_paths(&self) -> Vec<String> {
            self.get_paths.lock().unwrap().clone()
        }

        fn list_prefixes(&self) -> Vec<String> {
            self.list_prefixes.lock().unwrap().clone()
        }

        fn list_offsets(&self) -> Vec<String> {
            self.list_offsets.lock().unwrap().clone()
        }

        fn max_active_gets(&self) -> usize {
            self.max_active_gets
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn record_get_start(&self) {
            let active = self
                .active_gets
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let mut current = self
                .max_active_gets
                .load(std::sync::atomic::Ordering::SeqCst);
            while active > current {
                match self.max_active_gets.compare_exchange(
                    current,
                    active,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(observed) => current = observed,
                }
            }
        }

        fn record_get_end(&self) {
            self.active_gets
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl std::fmt::Debug for RecordingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RecordingObjectStore")
        }
    }

    impl std::fmt::Display for RecordingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("RecordingObjectStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RecordingObjectStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.put_paths.lock().unwrap().push(location.to_string());
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.get_paths.lock().unwrap().push(location.to_string());
            self.record_get_start();
            if !self.get_delay.is_zero() {
                sleep(self.get_delay).await;
            }
            let result = self.inner.get_opts(location, options).await;
            self.record_get_end();
            result
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.list_prefixes
                .lock()
                .unwrap()
                .push(prefix.map_or_else(String::new, ToString::to_string));
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&object_store::path::Path>,
            offset: &object_store::path::Path,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.list_prefixes
                .lock()
                .unwrap()
                .push(prefix.map_or_else(String::new, ToString::to_string));
            self.list_offsets.lock().unwrap().push(offset.to_string());
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn compactor_configured_object_store_builds_when_not_injected() {
        let object_store_dir = tempfile::tempdir().unwrap();
        let object_store_url = Url::from_directory_path(object_store_dir.path())
            .expect("temporary directory should be representable as a file URL")
            .to_string();
        let config = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "compactor",
            "--object-store-url",
            &object_store_url,
        ]);

        let configured_store = build_compactor_configured_object_store(&config, None)
            .expect("valid object-store URL should configure a compactor store");

        assert!(
            configured_store.is_some(),
            "compactor should build the configured object store when no store is injected"
        );
    }

    /// The `OTLP`/HTTP logs handler must decompress `Content-Encoding: gzip`
    /// before it protobuf-decodes. The OpenTelemetry SDK's `otlphttp` exporter,
    /// which the demo's Alloy uses, gzips by default, so a regression here
    /// means every emitted log line silently fails to decode, and no logs are
    /// ingested.
    #[test]
    fn normalize_otlp_http_logs_decodes_gzip_identically_to_identity() {
        use std::io::Write as _;

        use opentelemetry_proto::tonic::{
            logs::v1::{ResourceLogs, ScopeLogs},
            resource::v1::Resource,
        };

        let request = ProtoExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![ProtoKeyValue {
                        key: "service.name".to_string(),
                        value: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::StringValue(
                                "checkout".to_string(),
                            )),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![ProtoLogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        body: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::StringValue(
                                "hello world".to_string(),
                            )),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let raw = request.encode_to_vec();

        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "demo".parse().unwrap());
        headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

        // Identity (no Content-Encoding) decodes to a single record.
        let identity = normalize_otlp_http_logs(&headers, &raw, None, None)
            .expect("uncompressed OTLP proto logs should decode");
        assert_eq!(identity.len(), 1);
        assert_eq!(identity[0].line, "hello world");

        // The gzip-compressed body must decode to exactly the same records.
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&raw).unwrap();
        let gzipped = encoder.finish().unwrap();

        let mut gz_headers = headers.clone();
        gz_headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let from_gzip = normalize_otlp_http_logs(&gz_headers, &gzipped, None, None)
            .expect("gzip-compressed OTLP proto logs should decode");
        assert_eq!(from_gzip, identity);
    }

    fn hot_tail_test_record(timestamp_ns: i64, app: &str) -> WalLogRecord {
        WalLogRecord {
            tenant: "tenant".to_string(),
            labels: BTreeMap::from([("app".to_string(), app.to_string())]),
            timestamp_ns,
            line: format!("line@{timestamp_ns}"),
            structured_metadata: BTreeMap::new(),
            position: None,
        }
    }

    /// Brute-force oracle: the records a full linear scan keeps for an inclusive window.
    fn brute_force_in_range(
        records: &[WalLogRecord],
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<WalLogRecord> {
        records
            .iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .cloned()
            .collect()
    }

    /// The time-bucketed `records_in_range` MUST return exactly the same records (and
    /// so the same label/field sets) as a full-buffer scan, for any inclusive
    /// `[start, end]`, even though records are appended in NO timestamp order. This is the
    /// soundness guarantee that lets the query paths prune to the window instead of
    /// scanning the whole retained buffer.
    #[tokio::test]
    async fn hot_tail_records_in_range_matches_full_scan_under_out_of_order_inserts() {
        let bucket = minutes(1).nanos_i64();

        let hot_tail = BufferedLogHotTail::default();

        // Timestamps deliberately out of order and spread across many one-minute buckets,
        // with duplicates at the same instant and records straddling bucket boundaries.
        let timestamps = [
            5 * bucket + 10,
            bucket - 1, // last ns of bucket 0
            3 * bucket,
            bucket,          // first ns of bucket 1
            5 * bucket + 10, // duplicate timestamp
            0,
            7 * bucket + 42,
            2 * bucket + 999,
            3 * bucket, // duplicate timestamp in a different append position
            4 * bucket - 1,
            -bucket + 5, // a pre-epoch (negative) timestamp
            6 * bucket,
        ];
        let apps = ["api", "web", "db"];
        let records: Vec<WalLogRecord> = timestamps
            .iter()
            .enumerate()
            .map(|(i, &ts)| hot_tail_test_record(ts, apps[i % apps.len()]))
            .collect();

        // Append one at a time to exercise incremental bucket insertion of out-of-order data.
        for record in &records {
            hot_tail.append_records(vec![record.clone()]);
        }

        // `records()` must still return the full append-ordered buffer (the tail path
        // depends on this).
        assert_eq!(hot_tail.records(), records);

        // Probe a wide set of windows: exact bucket edges, sub-bucket slivers, windows
        // spanning many buckets, empty windows, and windows entirely outside the data.
        let min_ts = *timestamps.iter().min().unwrap();
        let max_ts = *timestamps.iter().max().unwrap();
        let mut probes: Vec<(i64, i64)> = Vec::new();
        // Walk window starts at a coarse quarter-bucket stride from below the earliest
        // record to above the latest, pairing each with several spans.
        let stride = bucket / 4;
        let mut start = min_ts - 2 * bucket;
        while start <= max_ts + 2 * bucket {
            for span in [0_i64, 1, bucket - 1, bucket, bucket + 1, 3 * bucket] {
                probes.push((start, start + span));
            }
            start += stride;
        }
        // Add exact per-record point windows and tight windows around each timestamp.
        for &ts in &timestamps {
            probes.push((ts, ts));
            probes.push((ts - 1, ts));
            probes.push((ts, ts + 1));
            probes.push((ts + 1, ts + 1));
        }

        for (start, end) in probes {
            if start > end {
                // Mirror the guard: an inverted window yields nothing.
                assert!(hot_tail.records_in_range(start, end).is_empty());
                continue;
            }
            let expected = brute_force_in_range(&records, start, end);
            let actual = hot_tail.records_in_range(start, end);
            assert_eq!(
                actual, expected,
                "records_in_range({start}, {end}) diverged from full-scan oracle"
            );

            // The label sets a query would derive must be identical too (records are the
            // sole input to label/field extraction).
            let expected_labels: BTreeSet<Labels> =
                expected.iter().map(|r| r.labels.clone()).collect();
            let actual_labels: BTreeSet<Labels> = actual.iter().map(|r| r.labels.clone()).collect();
            assert_eq!(
                actual_labels, expected_labels,
                "label sets diverged at [{start}, {end}]"
            );
        }

        // The trait-object path the querier actually uses must agree with the inherent method.
        let dyn_tail: Arc<dyn LogHotTail> = Arc::new(hot_tail.clone());
        let window = (2 * bucket, 6 * bucket);
        assert_eq!(
            dyn_tail.records_in_range(window.0, window.1),
            hot_tail.records_in_range(window.0, window.1),
        );

        // The default trait impl (used by other LogHotTail implementors, e.g. the
        // in-memory sink) falls back to filtering the full buffer and must also agree.
        let in_memory = InMemoryWalSink::default();
        for record in &records {
            LogWalSink::append(&in_memory, record.clone())
                .await
                .unwrap();
        }
        let in_memory_dyn: Arc<dyn LogHotTail> = Arc::new(in_memory);
        assert_eq!(
            in_memory_dyn.records_in_range(window.0, window.1),
            brute_force_in_range(&records, window.0, window.1),
        );
    }

    #[test]
    fn hot_tail_prune_compacted_rebuilds_records_and_time_index() {
        let bucket = minutes(1).nanos_i64();

        let hot_tail = BufferedLogHotTail::default();
        let mut compacted_by_offset = hot_tail_test_record(4 * bucket, "offset-old");
        compacted_by_offset.position = Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(7),
        });
        let mut kept_by_offset = hot_tail_test_record(3 * bucket, "offset-new");
        kept_by_offset.position = Some(WalPosition {
            partition: PartitionIndex(0),
            offset: Offset(8),
        });
        let compacted_by_time = hot_tail_test_record(2 * bucket, "time-old");
        let kept_by_time = hot_tail_test_record(5 * bucket, "time-new");
        let expected = vec![kept_by_offset.clone(), kept_by_time.clone()];

        hot_tail.append_records(vec![
            compacted_by_offset,
            kept_by_offset,
            compacted_by_time,
            kept_by_time,
        ]);

        let frontier =
            CompactionFrontier::new(2 * bucket).with_partition_offset(PartitionIndex(0), Offset(7));

        assert_eq!(hot_tail.prune_compacted(&frontier), 2);
        assert_eq!(hot_tail.records(), expected);
        assert2::assert!(hot_tail.records_in_range(0, 6 * bucket) == expected);
        assert!(hot_tail.records_in_range(2 * bucket, 2 * bucket).is_empty());
        assert!(hot_tail.records_in_range(4 * bucket, 4 * bucket).is_empty());
    }

    #[tokio::test]
    async fn compaction_frontier_refresh_prunes_hot_tail_from_object_store() {
        let store = object_store::memory::InMemory::new();
        let prefix = ObjectPath::default();
        let frontier = SharedCompactionFrontier::default();
        let hot_tail = BufferedLogHotTail::default();
        let compacted = hot_tail_test_record(1_000, "old");
        let fresh = hot_tail_test_record(3_000, "new");
        hot_tail.append_records(vec![compacted, fresh.clone()]);
        write_compaction_frontier_to_object_store(&store, &prefix, &CompactionFrontier::new(2_000))
            .await
            .unwrap();

        let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
            .await
            .unwrap();

        assert_eq!(pruned, 1);
        assert_eq!(frontier.snapshot(), CompactionFrontier::new(2_000));
        assert_eq!(hot_tail.records(), vec![fresh]);
    }

    #[tokio::test]
    async fn compaction_frontier_refresh_treats_absent_manifest_as_empty() {
        let store = object_store::memory::InMemory::new();
        let prefix = ObjectPath::default();
        let frontier = SharedCompactionFrontier::new(CompactionFrontier::new(123));
        let hot_tail = BufferedLogHotTail::default();
        let fresh = hot_tail_test_record(3_000, "new");
        hot_tail.append_records(vec![fresh.clone()]);

        let pruned = refresh_compaction_frontier_and_prune(&store, &prefix, &frontier, &hot_tail)
            .await
            .unwrap();

        assert_eq!(pruned, 0);
        assert_eq!(frontier.snapshot(), CompactionFrontier::new(123));
        assert_eq!(hot_tail.records(), vec![fresh]);
    }

    /// The shard catalog gains a compacted range once, and only once. Losing
    /// the push leaves a shard nobody can find; losing the containment test
    /// lists it twice, and the querier then reads the same shard twice.
    #[tokio::test]
    async fn the_shard_catalog_lists_each_compacted_range_exactly_once() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability");
        let tenant = "tenant-a";
        let range = TimeRange::new(300, 399).unwrap();
        let mut labels_index = LabelIndex::default();
        let api =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "api".into())]));
        let descriptor = BlockDescriptor::new(
            BlockKey::new(tenant, 0, 30, 39, range),
            BTreeSet::from([api]),
        );
        let mut block_index = BlockIndex::default();
        block_index.insert(descriptor.clone());

        for round in 1..=2 {
            write_tenant_compaction_indexes_to_object_store(
                &store,
                &prefix,
                tenant,
                &descriptor,
                &labels_index,
                &block_index,
                LogCompactionIndexOutput::FullManifestAndShardCatalog,
            )
            .await
            .unwrap();

            let catalog =
                read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, tenant)
                    .await
                    .unwrap();
            check!(catalog == vec![range], "after round {round}");
        }
    }

    #[tokio::test]
    async fn appending_log_index_shard_does_not_rewrite_historical_shards_or_full_manifest() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability");
        let tenant = "tenant-a";
        let old_range_a = TimeRange::new(100, 199).unwrap();
        let old_range_b = TimeRange::new(200, 299).unwrap();
        let new_range = TimeRange::new(300, 399).unwrap();
        let mut labels_index = LabelIndex::default();
        let api =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "api".into())]));
        let worker =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "worker".into())]));
        let admin =
            labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "admin".into())]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 10, 19, old_range_a),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 20, 29, old_range_b),
            BTreeSet::from([worker]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[old_range_a, old_range_b],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();

        let new_descriptor = BlockDescriptor::new(
            BlockKey::new(tenant, 0, 30, 39, new_range),
            BTreeSet::from([admin]),
        );
        block_index.insert(new_descriptor.clone());
        store.clear_put_paths();

        write_tenant_compaction_indexes_to_object_store(
            &store,
            &prefix,
            tenant,
            &new_descriptor,
            &labels_index,
            &block_index,
            LogCompactionIndexOutput::ShardManifests,
        )
        .await
        .unwrap();

        // Exactly one PUT is allowed: the new shard manifest. The global
        // tenant manifest, the shard catalog, and the old shard manifests
        // must not be rewritten.
        let put_paths = store.put_paths();
        assert_eq!(
            put_paths,
            vec![
                krabka_blockstore::log_tenant_index_shard_manifest_object_path(
                    &prefix, tenant, new_range
                )
                .to_string()
            ],
            "only the new shard manifest should be written"
        );
    }

    #[test]
    fn detected_labels_empty_query_is_match_all() {
        // Grafana's Logs Drilldown loads `detected_labels?query=` with an empty
        // query to discover every label. An empty/blank query must parse to
        // `None` (match all streams), not be handed to the LogQL parser — which
        // rejects "" with `syntax error: unexpected $end, expecting '{'`.
        for raw in ["query=", "query=%20", "query=%20%20"] {
            let params = parse_detected_labels_params(Some(raw)).unwrap();
            assert!(params.query.is_none(), "{raw}: {:?}", params.query);
        }
        // A real stream selector is still preserved.
        let params = parse_detected_labels_params(Some("query=%7Bapp%3D%22api%22%7D")).unwrap();
        assert_eq!(params.query.as_deref(), Some(r#"{app="api"}"#));
    }

    #[test]
    fn instant_synthetic_vector_uses_raw_loki_timestamp() {
        let response = loki_instant_scalar_or_vector_response(
            4_000_000_000,
            ScalarVectorExpressionResult::Vector {
                sample: Some("1".to_string()),
                metric: BTreeMap::new(),
            },
        );

        assert_eq!(
            response["data"]["result"][0]["value"][0],
            json!(4_000_000_000i64)
        );
    }

    #[test]
    fn instant_scalar_expression_keeps_loki_seconds_timestamp() {
        let response = loki_instant_scalar_or_vector_response(
            4_000_000_000,
            ScalarVectorExpressionResult::Scalar {
                sample: "2".to_string(),
            },
        );

        assert_eq!(response["data"]["result"][0], json!(4));
    }

    #[test]
    fn formats_loki_numeric_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[1000000000,"non-string push timestamp"]]}]}"#;
        let timestamp = json!(1_000_000_000);
        let line = json!("non-string push timestamp");

        assert_eq!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|alues\":[[1000000000,\"non-string push timestamp\"]]}]}|...\n"
        );
    }

    #[test]
    fn formats_loki_object_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[{"ts":"1000000000"},"object push timestamp"]]}]}"#;
        let timestamp = json!({"ts": "1000000000"});
        let line = json!("object push timestamp");

        assert_eq!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|\":[[{\"ts\":\"1000000000\"},\"object push timestamp\"]]}]}|...\n"
        );
    }

    #[test]
    fn formats_loki_array_json_timestamp_error_context() {
        let body = br#"{"streams":[{"stream":{"app":"api"},"values":[[["1000000000"],"array push timestamp"]]}]}"#;
        let timestamp = json!(["1000000000"]);
        let line = json!("array push timestamp");

        assert_eq!(
            loki_json_timestamp_value_parse_error(body, &timestamp, Some(&line)),
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}' symbol, error found in #10 byte of ...|estamp\"]]}]}|..., bigger context ...|values\":[[[\"1000000000\"],\"array push timestamp\"]]}]}|...\n"
        );
    }

    // --- FIX B1 tests ---

    /// A `TenantObjectStoreManifest` source backed by an empty in-memory
    /// store, with no manifest present, must return Ok with an empty
    /// self-clone index. It must not propagate `NotFound` as an error.
    #[tokio::test]
    async fn querier_state_with_request_tenant_index_tolerates_absent_manifest() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::default();

        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_manifest(store, prefix);

        let query_range = TimeRange::new(0, 1).unwrap();
        let result = state
            .with_request_tenant_index("test-tenant", query_range)
            .await;

        assert!(
            result.is_ok(),
            "expected Ok on absent cold index manifest, got: {:?}",
            result.err()
        );
        let returned = result.unwrap();
        assert!(
            returned.block_index.blocks().is_empty(),
            "expected empty block index when no manifest exists"
        );
    }

    /// Same check for the `TenantObjectStoreShards` variant.
    #[tokio::test]
    async fn querier_state_with_request_tenant_index_tolerates_absent_shards() {
        use object_store::memory::InMemory;

        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = ObjectPath::default();

        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_dynamic_tenant_object_store_shards(store, prefix);

        let query_range = TimeRange::new(0, 1).unwrap();
        let result = state
            .with_request_tenant_index("test-tenant", query_range)
            .await;

        assert!(
            result.is_ok(),
            "expected Ok on absent cold index shards, got: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_caches_shard_indexes_for_repeated_range() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_range = TimeRange::new(0, 100).unwrap();
        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();
        let shard_range = TimeRange::new(10, 19).unwrap();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range),
            BTreeSet::from([api]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[shard_range],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let first = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        assert_eq!(
            first.label_index.label_names(tenant),
            BTreeSet::from(["app".to_string()])
        );
        assert_eq!(
            second.label_index.label_names(tenant),
            BTreeSet::from(["app".to_string()])
        );

        let shard_prefix =
            krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
        let shard_manifest = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range,
        )
        .to_string();
        let list_count = store
            .list_prefixes()
            .into_iter()
            .filter(|prefix| prefix == &shard_prefix)
            .count();
        let shard_get_count = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest)
            .count();

        assert!(list_count == 1, "shard prefix should be listed once");
        assert!(
            shard_get_count == 1,
            "shard manifest should be fetched once"
        );
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_reuses_shard_indexes_for_moving_ranges() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let first_query_range = TimeRange::new(0, 100).unwrap();
        let moving_query_range = TimeRange::new(5, 105).unwrap();
        let shard_range_a = TimeRange::new(10, 19).unwrap();
        let shard_range_b = TimeRange::new(80, 89).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let worker =
            labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "worker")]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, shard_range_a),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 44, 45, shard_range_b),
            BTreeSet::from([worker]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[shard_range_a, shard_range_b],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let first = state
            .with_request_tenant_index(tenant, first_query_range)
            .await
            .unwrap();
        let second = state
            .with_request_tenant_index(tenant, moving_query_range)
            .await
            .unwrap();

        for state in [&first, &second] {
            check!(state.label_index.label_names(tenant) == BTreeSet::from(["app".to_string()]));
            check!(state.block_index.blocks().len() == 2);
        }

        let shard_prefix =
            krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
        let shard_manifest_a = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range_a,
        )
        .to_string();
        let shard_manifest_b = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
            &prefix,
            tenant,
            shard_range_b,
        )
        .to_string();
        let list_count = store
            .list_prefixes()
            .into_iter()
            .filter(|prefix| prefix == &shard_prefix)
            .count();
        let shard_get_count_a = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest_a)
            .count();
        let shard_get_count_b = store
            .get_paths()
            .into_iter()
            .filter(|path| path == &shard_manifest_b)
            .count();

        check!(list_count == 1, "shard prefix should be listed once");
        check!(
            shard_get_count_a == 1,
            "shard manifest A should be fetched once"
        );
        check!(
            shard_get_count_b == 1,
            "shard manifest B should be fetched once"
        );
    }

    #[tokio::test]
    async fn querier_state_with_request_tenant_index_lists_shards_from_query_window_offset() {
        let store = RecordingObjectStore::new();
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let query_start = 1_700_000_000_000_000_000;
        let query_end = query_start + 300_000_000_000;
        let query_range = TimeRange::new(query_start, query_end).unwrap();
        let old_shard_range =
            TimeRange::new(query_start - 600_000_000_000, query_start - 599_000_000_000).unwrap();
        let matching_shard_range = TimeRange::new(query_start + 10, query_start + 20).unwrap();

        let mut labels_index = LabelIndex::default();
        let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 40, 41, old_shard_range),
            BTreeSet::from([api]),
        ));
        block_index.insert(BlockDescriptor::new(
            BlockKey::new(tenant, 0, 42, 43, matching_shard_range),
            BTreeSet::from([api]),
        ));
        krabka_blockstore::write_tenant_log_index_shards_to_object_store(
            &store,
            &prefix,
            tenant,
            &[old_shard_range, matching_shard_range],
            &labels_index,
            &block_index,
        )
        .await
        .unwrap();
        store.clear_recorded_paths();

        let state = QuerierState::new(
            tempfile::tempdir().unwrap().keep(),
            LabelIndex::default(),
            BlockIndex::default(),
        )
        .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

        let state = state
            .with_request_tenant_index(tenant, query_range)
            .await
            .unwrap();

        assert_eq!(
            state.label_index.label_names(tenant),
            BTreeSet::from(["app".to_string()])
        );
        let expected_offset =
            krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant)
                .join(format!("time={}", query_start - (query_end - query_start)))
                .to_string();
        assert!(
            store.list_offsets().contains(&expected_offset),
            "shard listing should start near the query window; offsets={:?}",
            store.list_offsets()
        );
    }

    #[test]
    fn metadata_index_range_defaults_empty_metadata_requests_to_recent_window() {
        const SIX_HOURS_NS: i64 = 6 * 60 * 60 * 1_000_000_000;
        let before = current_unix_time_ns();
        let range = metadata_index_range(&SeriesParams::default()).unwrap();
        let after = current_unix_time_ns();

        check!(
            range.start_ns >= before - SIX_HOURS_NS,
            "default metadata index start should be within Loki's default recent window"
        );
        check!(
            range.end_ns <= after,
            "default metadata index end should be now-ish, got {} after {}",
            range.end_ns,
            after
        );
        check!(
            range.end_ns - range.start_ns <= SIX_HOURS_NS,
            "default metadata index range should not be all time"
        );
    }

    #[tokio::test]
    async fn object_store_stream_query_batches_cold_block_reads() {
        let store = RecordingObjectStore::new().with_get_delay(Duration::from_millis(25));
        let prefix = ObjectPath::from("observability/logs");
        let tenant = "tenant-a";
        let mut label_index = LabelIndex::default();
        let api = label_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
        let mut block_index = BlockIndex::default();

        for block_id in 0_i64..4 {
            let start_ns = block_id * 10;
            let end_ns = start_ns + 9;
            let block = write_log_block_to_object_store(
                &store,
                &prefix,
                &BlockKey::new(
                    tenant,
                    0,
                    start_ns,
                    end_ns,
                    TimeRange::new(start_ns, end_ns).unwrap(),
                ),
                vec![LogRow::new(
                    api,
                    end_ns,
                    format!("api error {block_id}"),
                    BTreeMap::new(),
                )],
            )
            .await
            .unwrap();
            block_index.insert(block);
        }

        let plan = plan_stream_query(
            tenant,
            TimeRange::new(0, 39).unwrap(),
            parse_query(r#"{app="api"} |= "error""#).unwrap(),
            &label_index,
            &block_index,
        )
        .unwrap();

        let scan = execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options(
            Arc::new(store.clone()),
            &prefix,
            &plan,
            &label_index,
            QueryHotTail {
                records: &[],
                frontier: &CompactionFrontier::new(i64::MAX),
                delete_filters: &[],
            },
            StreamScanOptions::from_stream_options(LokiDirection::Forward, Some(100), None, None),
        )
        .await
        .unwrap();

        assert_eq!(scan.scanned_blocks.len(), 4);
        assert!(
            store.max_active_gets() > 1,
            "expected cold block reads to overlap, max_active_gets={}",
            store.max_active_gets()
        );
    }

    // --- FIX B3 tests ---

    /// `connect_with_startup_retry` returns Ok immediately when the closure succeeds on the first try.
    #[tokio::test]
    async fn connect_with_startup_retry_succeeds_on_first_try() {
        let result: Result<u32, String> =
            connect_with_startup_retry("test", secs(5), secs(1), millis(1), millis(10), || async {
                Ok::<u32, String>(42)
            })
            .await;

        assert_eq!(result.unwrap(), 42);
    }

    /// `connect_with_startup_retry` retries on failure and returns Ok when a later retry succeeds.
    #[tokio::test]
    async fn connect_with_startup_retry_retries_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering as AO};
        let counter = Arc::new(AtomicU32::new(0));
        let counter2 = counter.clone();

        let result: Result<u32, String> = connect_with_startup_retry(
            "test",
            secs(10),
            secs(1),
            millis(1),
            millis(10),
            move || {
                let c = counter2.clone();
                async move {
                    let n = c.fetch_add(1, AO::SeqCst);
                    if n < 2 {
                        Err(format!("not ready yet (attempt {n})"))
                    } else {
                        Ok(99u32)
                    }
                }
            },
        )
        .await;

        assert_eq!(result.unwrap(), 99);
        assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
    }

    /// `connect_with_startup_retry` returns the error after the deadline is exceeded.
    #[tokio::test]
    async fn connect_with_startup_retry_gives_up_after_deadline() {
        let result: Result<u32, String> = connect_with_startup_retry(
            "test",
            millis(50), // very short deadline
            millis(10),
            millis(1),
            millis(10),
            || async { Err::<u32, String>("always fails".to_string()) },
        )
        .await;

        assert!(result.is_err(), "expected Err after deadline");
        assert_eq!(result.unwrap_err(), "always fails");
    }

    fn acl_entry(
        resource_type: ResourceType,
        resource_name: &str,
        pattern_type: PatternType,
        principal: &str,
        operation: AclOperation,
        permission_type: PermissionType,
    ) -> AclEntry {
        AclEntry {
            resource_type,
            resource_name: resource_name.to_string(),
            pattern_type,
            principal: principal.to_string(),
            host: "*".to_string(),
            operation,
            permission_type,
        }
    }

    #[test]
    fn missing_timestamp_fallback_age_is_exact() {
        check!(LOKI_REJECT_OLD_SAMPLES_MAX_AGE == hours(168));
    }

    #[test]
    fn distributor_policy_uses_defaults_and_cli_overrides() {
        let defaults =
            ServiceConfig::parse_from(["krabka-observability", "--target", "distributor"]);
        check!(defaults.reject_old_samples_max_age == days(7));
        check!(defaults.creation_grace_period == minutes(10));
        check!(defaults.ingest_quota_burst_window == secs(1));
        check!(defaults.wal_connect_startup_deadline == minutes(2));
        check!(defaults.wal_connect_attempt_timeout == secs(15));
        check!(defaults.wal_connect_initial_backoff == millis(200));
        check!(defaults.wal_connect_max_backoff == secs(2));

        let configured = ServiceConfig::try_parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            "--reject-old-samples-max-age=8d",
            "--creation-grace-period=11m",
            "--ingest-quota-burst-window=2s",
            "--wal-connect-startup-deadline=3m",
            "--wal-connect-attempt-timeout=16s",
            "--wal-connect-initial-backoff=300ms",
            "--wal-connect-max-backoff=3s",
        ])
        .expect("valid distributor policy");
        check!(configured.reject_old_samples_max_age == days(8));
        check!(configured.creation_grace_period == minutes(11));
        check!(configured.ingest_quota_burst_window == secs(2));
        check!(configured.wal_connect_startup_deadline == minutes(3));
        check!(configured.wal_connect_attempt_timeout == secs(16));
        check!(configured.wal_connect_initial_backoff == millis(300));
        check!(configured.wal_connect_max_backoff == secs(3));
    }

    #[test]
    fn distributor_policy_rejects_zero_and_invalid_bounds() {
        for argument in [
            "--reject-old-samples-max-age=0s",
            "--creation-grace-period=0s",
            "--ingest-quota-burst-window=0s",
            "--wal-connect-startup-deadline=0s",
            "--wal-connect-attempt-timeout=0s",
            "--wal-connect-initial-backoff=0s",
            "--wal-connect-max-backoff=0s",
        ] {
            check!(
                ServiceConfig::try_parse_from([
                    "krabka-observability",
                    "--target",
                    "distributor",
                    argument,
                ])
                .is_err(),
                "accepted {argument}"
            );
        }

        let attempt_above_deadline = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            "--wal-connect-startup-deadline=1s",
            "--wal-connect-attempt-timeout=2s",
        ]);
        check!(validate_distributor_policy(&attempt_above_deadline).is_err());

        let initial_above_max = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            "--wal-connect-initial-backoff=2s",
            "--wal-connect-max-backoff=1s",
        ]);
        check!(validate_distributor_policy(&initial_above_max).is_err());

        // Equal is not "exceeds". Both cases above are rejections, so the
        // comparisons could have refused a timeout that merely *matches* its
        // deadline and nothing would have noticed.
        for (deadline, timeout) in [
            (
                "--wal-connect-startup-deadline=1s",
                "--wal-connect-attempt-timeout=1s",
            ),
            (
                "--wal-connect-initial-backoff=1s",
                "--wal-connect-max-backoff=1s",
            ),
        ] {
            let at_the_limit = ServiceConfig::parse_from([
                "krabka-observability",
                "--target",
                "distributor",
                deadline,
                timeout,
            ]);
            check!(
                validate_distributor_policy(&at_the_limit).is_ok(),
                "{deadline} with {timeout}"
            );
        }
    }

    #[tokio::test]
    async fn distributor_dependency_startup_rejects_invalid_policy_before_connecting() {
        let config = ServiceConfig::parse_from([
            "krabka-observability",
            "--target",
            "distributor",
            "--wal-bootstrap-server=127.0.0.1:1",
            "--wal-connect-startup-deadline=1s",
            "--wal-connect-attempt-timeout=2s",
        ]);

        let Err(error) = build_service_dependencies(&config).await else {
            panic!("invalid policy must fail before broker connection");
        };
        check!(
            error
                .to_string()
                .contains("must not exceed startup deadline")
        );
    }

    #[test]
    fn compactor_policy_uses_defaults_and_cli_overrides() {
        let defaults = ServiceConfig::default();
        check!(defaults.compactor_wal_poll_timeout == millis(500));
        check!(defaults.compactor_accumulation_window == secs(2));
        check!(defaults.compactor_accumulation_poll_timeout == millis(250));
        check!(defaults.compactor_max_records_per_batch.get() == 4096);
        check!(defaults.compactor_idle_interval == millis(10));
        check!(defaults.compactor_object_store_initial_backoff == millis(10));
        check!(defaults.compactor_object_store_max_backoff == millis(500));

        let configured = ServiceConfig::try_parse_from([
            "krabka-observability",
            "--target=compactor",
            "--compactor-wal-poll-timeout=600ms",
            "--compactor-accumulation-window=3s",
            "--compactor-accumulation-poll-timeout=300ms",
            "--compactor-max-records-per-batch=5000",
            "--compactor-idle-interval=20ms",
            "--compactor-object-store-initial-backoff=20ms",
            "--compactor-object-store-max-backoff=600ms",
        ])
        .expect("valid compactor policy");
        check!(configured.compactor_wal_poll_timeout == millis(600));
        check!(configured.compactor_accumulation_window == secs(3));
        check!(configured.compactor_accumulation_poll_timeout == millis(300));
        check!(configured.compactor_max_records_per_batch.get() == 5000);
        check!(configured.compactor_idle_interval == millis(20));
        check!(configured.compactor_object_store_initial_backoff == millis(20));
        check!(configured.compactor_object_store_max_backoff == millis(600));
    }

    #[test]
    fn compactor_policy_rejects_zero_and_invalid_bounds() {
        for argument in [
            "--compactor-wal-poll-timeout=0s",
            "--compactor-accumulation-window=0s",
            "--compactor-accumulation-poll-timeout=0s",
            "--compactor-max-records-per-batch=0",
            "--compactor-idle-interval=0s",
            "--compactor-object-store-initial-backoff=0s",
            "--compactor-object-store-max-backoff=0s",
        ] {
            check!(
                ServiceConfig::try_parse_from([
                    "krabka-observability",
                    "--target=compactor",
                    argument,
                ])
                .is_err(),
                "accepted {argument}"
            );
        }

        let poll_above_window = ServiceConfig::parse_from([
            "krabka-observability",
            "--target=compactor",
            "--compactor-accumulation-window=1s",
            "--compactor-accumulation-poll-timeout=2s",
        ]);
        check!(validate_compactor_policy(&poll_above_window).is_err());

        let initial_above_max = ServiceConfig::parse_from([
            "krabka-observability",
            "--target=compactor",
            "--compactor-object-store-initial-backoff=2s",
            "--compactor-object-store-max-backoff=1s",
        ]);
        check!(validate_compactor_policy(&initial_above_max).is_err());

        // And the same pair of boundaries here.
        for (window, timeout) in [
            (
                "--compactor-accumulation-window=1s",
                "--compactor-accumulation-poll-timeout=1s",
            ),
            (
                "--compactor-object-store-initial-backoff=1s",
                "--compactor-object-store-max-backoff=1s",
            ),
        ] {
            let at_the_limit = ServiceConfig::parse_from([
                "krabka-observability",
                "--target=compactor",
                window,
                timeout,
            ]);
            check!(
                validate_compactor_policy(&at_the_limit).is_ok(),
                "{window} with {timeout}"
            );
        }
    }

    #[test]
    fn querier_policy_uses_defaults_and_cli_overrides() {
        let defaults = ServiceConfig::default();
        check!(defaults.querier_frontier_refresh_interval == secs(5));
        check!(defaults.querier_dynamic_index_cache_ttl == secs(5));
        check!(defaults.querier_shard_index_cache_ttl == minutes(5));
        check!(defaults.querier_shard_fetch_concurrency.get() == 32);
        check!(defaults.querier_cold_block_fetch_concurrency.get() == 8);
        check!(defaults.querier_hot_tail_bucket_width == minutes(1));
        check!(defaults.querier_hot_tail_interval == millis(50));
        check!(defaults.querier_dependency_reconnect_interval == millis(500));

        let configured = ServiceConfig::try_parse_from([
            "krabka-observability",
            "--target=querier",
            "--querier-frontier-refresh-interval=6s",
            "--querier-dynamic-index-cache-ttl=7s",
            "--querier-shard-index-cache-ttl=6m",
            "--querier-shard-fetch-concurrency=33",
            "--querier-cold-block-fetch-concurrency=9",
            "--querier-hot-tail-bucket-width=2m",
            "--querier-hot-tail-interval=60ms",
            "--querier-dependency-reconnect-interval=600ms",
        ])
        .expect("valid querier policy");
        check!(configured.querier_frontier_refresh_interval == secs(6));
        check!(configured.querier_dynamic_index_cache_ttl == secs(7));
        check!(configured.querier_shard_index_cache_ttl == minutes(6));
        check!(configured.querier_shard_fetch_concurrency.get() == 33);
        check!(configured.querier_cold_block_fetch_concurrency.get() == 9);
        check!(configured.querier_hot_tail_bucket_width == minutes(2));
        check!(configured.querier_hot_tail_interval == millis(60));
        check!(configured.querier_dependency_reconnect_interval == millis(600));

        let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
            .with_runtime_policy(&configured);
        check!(state.dynamic_index_cache.cache_ttl == secs(7));
        check!(state.dynamic_index_cache.shard_cache_ttl == minutes(6));
        check!(state.dynamic_index_cache.shard_fetch_concurrency.get() == 33);
        check!(state.cold_block_fetch_concurrency.get() == 9);
        check!(
            StreamScanOptions::exhaustive()
                .with_block_fetch_concurrency(state.cold_block_fetch_concurrency)
                .block_fetch_concurrency()
                == 9
        );
    }

    #[test]
    fn querier_policy_rejects_zero() {
        for argument in [
            "--querier-frontier-refresh-interval=0s",
            "--querier-dynamic-index-cache-ttl=0s",
            "--querier-shard-index-cache-ttl=0s",
            "--querier-shard-fetch-concurrency=0",
            "--querier-cold-block-fetch-concurrency=0",
            "--querier-hot-tail-bucket-width=0s",
            "--querier-hot-tail-interval=0s",
            "--querier-dependency-reconnect-interval=0s",
        ] {
            check!(
                ServiceConfig::try_parse_from([
                    "krabka-observability",
                    "--target=querier",
                    argument,
                ])
                .is_err(),
                "accepted {argument}"
            );
        }
    }

    #[test]
    fn service_dependencies_builder_methods_preserve_existing_fields() {
        #[derive(Clone)]
        struct TestLimiter;
        #[async_trait]
        impl LogIngestLimiter for TestLimiter {
            async fn check(
                &self,
                _tenant: &str,
                _records: &[WalLogRecord],
            ) -> Result<(), IngestLimitError> {
                Ok(())
            }
        }

        #[derive(Clone)]
        struct TestAuthorizer;
        #[async_trait]
        impl LogQueryAuthorizer for TestAuthorizer {
            async fn check(&self, _tenant: &str) -> Result<(), QueryAuthorizationError> {
                Ok(())
            }
        }

        let metrics = ServiceMetrics::new();
        let frontier = SharedCompactionFrontier::default();
        let client_resource_policy = ClientResourcePolicy {
            dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(7)
                .unwrap(),
            frame_max: krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32))
                .unwrap(),
        };
        let deps = ServiceDependencies::default()
            .with_metrics(metrics.clone())
            .with_wal_sink(InMemoryWalSink::default())
            .with_ingest_limiter(TestLimiter)
            .with_query_authorizer(TestAuthorizer)
            .with_hot_tail_shared_frontier(BufferedLogHotTail::default(), frontier.clone())
            .with_deferred_wal_consumer_connect(
                "broker:9092".to_string(),
                "group".to_string(),
                "topic".to_string(),
                client_resource_policy,
            );

        check!(deps.metrics.is_some());
        check!(deps.wal_sink.is_some());
        check!(deps.ingest_limiter.is_some());
        check!(deps.query_authorizer.is_some());
        check!(deps.hot_tail.is_some());
        check!(deps.deferred_wal_consumer_connect.is_some());
        check!(Arc::ptr_eq(
            &deps.metrics.as_ref().unwrap().registry,
            &metrics.registry
        ));
        match deps.hot_tail.as_ref().unwrap().frontier.clone() {
            CompactionFrontierSource::Shared(actual) => {
                assert_eq!(actual.snapshot(), frontier.snapshot());
            }
            CompactionFrontierSource::Snapshot(_) => panic!("expected shared frontier"),
        }
        let deferred = deps.deferred_wal_consumer_connect.as_ref().unwrap();
        assert_eq!(deferred.bootstrap, "broker:9092");
        assert_eq!(deferred.group_id, "group");
        assert_eq!(deferred.topic, "topic");
        assert_eq!(deferred.client_resource_policy, client_resource_policy);
        let options = admin_connection_options(client_resource_policy);
        assert_eq!(
            options.dispatch_queue_capacity,
            client_resource_policy.dispatch_queue_capacity
        );
        assert_eq!(options.frame_max, client_resource_policy.frame_max);
    }

    #[test]
    fn retry_backoff_doubles_and_caps() {
        for (current, want) in [
            (millis(10), millis(20)),
            (millis(300), millis(500)),
            (millis(500), millis(500)),
        ] {
            check!(next_compactor_object_store_backoff(current, millis(500)) == want);
        }
        check!(next_compactor_object_store_backoff(millis(300), millis(400)) == millis(400));
    }

    #[test]
    fn acl_helpers_require_topic_operation_principal_and_pattern() {
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
            check_tenant_wal_write_acl(
                "tenant-a",
                "__krabka_observability_logs_wal",
                &[deny_write]
            )
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

    #[test]
    fn ingest_quota_bucket_and_byte_accounting_are_precise() {
        let record = WalLogRecord {
            tenant: "tenant-a".to_string(),
            labels: BTreeMap::from([
                ("app".to_string(), "api".to_string()),
                ("env".to_string(), "prod".to_string()),
            ]),
            timestamp_ns: 42,
            line: "hello".to_string(),
            structured_metadata: BTreeMap::from([("trace_id".to_string(), "abc".to_string())]),
            position: None,
        };
        let expected_bytes = "tenant-a".len()
            + "hello".len()
            + std::mem::size_of::<i64>()
            + "app".len()
            + "api".len()
            + "env".len()
            + "prod".len()
            + "trace_id".len()
            + "abc".len();
        check!(ingest_quota_bytes(&[record]) == measured_size(expected_bytes));

        let mut bucket = IngestQuotaBucket::new(bytes_per_sec(10), secs(1));
        check!(bucket.capacity() == bytes(10));
        check!(bucket.consume(bytes(10)));
        check!(!bucket.consume(ByteSize::from_bytes_f64(0.1)));
        bucket.update_rate(bytes_per_sec(5));
        check!(bucket.available <= bytes(5));
        bucket.available = bytes(4);
        bucket.update_rate(bytes_per_sec(20));
        check!(bucket.available >= bytes(4));
        check!(bucket.consume(bytes(4)));

        // Neither assertion above reaches the clamp: the bucket is empty by
        // then, so nothing is banked over the new capacity and `available`
        // could have been left alone -- or topped up to the new capacity --
        // without either inequality noticing.
        //
        // Lowering the rate shrinks the capacity, and what was banked above it
        // is given up.
        bucket.available = bytes(20);
        bucket.update_rate(bytes_per_sec(5));
        check!(bucket.available == bytes(5), "clamped to the new capacity");

        // Raising it grows the capacity and hands out nothing: a bucket
        // refills over time, not on a configuration change. The bound is loose
        // by a byte because `update_rate` refills first, over however long the
        // two statements took.
        bucket.available = bytes(2);
        bucket.update_rate(bytes_per_sec(50));
        check!(
            bucket.available < bytes(3),
            "not topped up to the new capacity"
        );

        // Refilling adds the rate over however long has passed. Every case
        // above reaches it only through `update_rate`, which calls it against
        // a bucket whose clock has not moved -- so with the body removed they
        // all behave the same.
        let mut refilling = IngestQuotaBucket::new(bytes_per_sec(10), secs(1));
        refilling.available = bytes(0);
        refilling.updated_at = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(500))
            .expect("the process has been running for at least half a second");
        refilling.refill();
        check!(
            refilling.available >= bytes(5),
            "half a second at ten bytes a second is at least five bytes"
        );
        check!(
            refilling.available <= bytes(10),
            "and never past the capacity"
        );

        let bucket = IngestQuotaBucket::new(bytes_per_sec(10), secs(2));
        check!(bucket.capacity() == bytes(20));
    }

    #[test]
    fn hot_tail_bucket_key_uses_euclidean_minutes() {
        let bucket_width = minutes(1);
        assert_eq!(hot_tail_bucket_key(0, bucket_width), 0);
        check!(hot_tail_bucket_key(bucket_width.nanos_i64() - 1, bucket_width) == 0);
        check!(hot_tail_bucket_key(bucket_width.nanos_i64(), bucket_width) == 1);
        assert_eq!(hot_tail_bucket_key(-1, bucket_width), -1);
        check!(hot_tail_bucket_key(-bucket_width.nanos_i64(), bucket_width) == -1);
        check!(hot_tail_bucket_key(-bucket_width.nanos_i64() - 1, bucket_width) == -2);
        check!(hot_tail_bucket_key(minutes(2).nanos_i64(), minutes(2)) == 1);
    }

    /// The buffer answers a range query through its bucket index, and nothing
    /// had exercised that path. The index is a granularity, not a filter: the
    /// exact bound is applied within the buckets it scans, so what has to hold
    /// is that no record in the window is left behind in a bucket the scan
    /// skipped.
    #[test]
    fn a_hot_tail_buffer_range_query_loses_no_record_to_its_buckets() {
        let minute = minutes(1).nanos_i64();
        let record = |timestamp_ns: i64| WalLogRecord {
            tenant: "t".to_string(),
            labels: Labels::default(),
            timestamp_ns,
            line: timestamp_ns.to_string(),
            structured_metadata: BTreeMap::new(),
            position: None,
        };

        let tail = super::BufferedLogHotTail::with_bucket_width(minutes(1));
        tail.append_records(vec![record(0), record(minute), record(minute * 2)]);

        let stamps = |start: i64, end: i64| {
            tail.records_in_range(start, end)
                .into_iter()
                .map(|record| record.timestamp_ns)
                .collect::<Vec<_>>()
        };
        check!(tail.records().len() == 3, "every record is kept");
        check!(
            stamps(0, minute) == vec![0, minute],
            "both ends are inclusive"
        );
        check!(
            stamps(1, minute - 1) == Vec::<i64>::new(),
            "a window between two records holds neither"
        );
        check!(
            stamps(0, minute * 2) == vec![0, minute, minute * 2],
            "a window spanning every bucket returns every record"
        );
    }

    /// Whether a compactor run failed on the object store decides whether the
    /// run is retried, and every variant that is not one has to say so. With
    /// the classifier stuck at true, a decode failure or a missing commit
    /// position would be retried forever.
    #[test]
    fn only_an_object_store_compactor_error_is_classified_as_one() {
        use super::{CompactionFrontierStoreError, CompactorRunError};

        check!(super::compactor_run_error_is_object_store(
            &CompactorRunError::Frontier(CompactionFrontierStoreError::ObjectStore(
                object_store::Error::NotFound {
                    path: "p".to_string(),
                    source: "gone".into(),
                }
            ))
        ));
        check!(!super::compactor_run_error_is_object_store(
            &CompactorRunError::MissingCommitPosition
        ));
        check!(!super::compactor_run_error_is_object_store(
            &CompactorRunError::Frontier(CompactionFrontierStoreError::InvalidVersion {
                expected: 1,
                actual: 2,
            })
        ));
    }

    /// Accumulating a WAL batch stops on two conditions, and neither had ever
    /// been reached. An empty first poll returns straight away rather than
    /// waiting out the accumulation window for records that are not coming;
    /// and once the batch is full the loop stops, rather than taking one more
    /// poll's worth beyond the cap it was given.
    #[tokio::test]
    async fn accumulating_a_wal_batch_stops_when_empty_or_full() {
        struct ScriptedConsumer {
            batches: std::collections::VecDeque<Vec<WalRecordForTest>>,
        }
        type WalRecordForTest = super::KafkaWalRecord;

        #[async_trait]
        impl super::LogWalConsumer for ScriptedConsumer {
            async fn poll(
                &mut self,
                _timeout: Time,
            ) -> Result<Vec<super::KafkaWalRecord>, super::WalConsumerError> {
                Ok(self.batches.pop_front().unwrap_or_default())
            }
            async fn commit_compacted(
                &mut self,
                _position: super::WalPosition,
            ) -> Result<(), super::WalConsumerError> {
                Ok(())
            }
        }

        let record = |offset: i64| super::KafkaWalRecord {
            value: Vec::new(),
            partition: PartitionIndex(0),
            offset: Offset(offset),
            timestamp_ms: None,
            headers: Vec::new(),
        };
        let poll = |batches: Vec<Vec<super::KafkaWalRecord>>, max: usize| async move {
            let mut consumer = ScriptedConsumer {
                batches: batches.into_iter().collect(),
            };
            super::poll_accumulated_log_compaction_records(
                &mut consumer,
                secs(1),
                secs(5),
                millis(10),
                NonZeroUsize::new(max).expect("a positive cap"),
            )
            .await
            .expect("the scripted consumer does not fail")
        };

        // An empty first poll is the answer, not the start of a wait: the
        // batch waiting behind it must not be drawn in.
        let empty = poll(vec![vec![], vec![record(1)]], 3).await;
        check!(empty.is_empty(), "an empty poll returns empty");

        // One short of the cap accumulates; reaching the cap stops, leaving
        // the batch behind it alone.
        let full = poll(
            vec![vec![record(1)], vec![record(2), record(3)], vec![record(4)]],
            3,
        )
        .await;
        check!(full.len() == 3, "stops at the cap, got {}", full.len());
    }

    #[test]
    fn native_header_detection_requires_native_log_shape() {
        for (key, value, want) in [
            ("krabka-wal-record-type", Some(&b"log-line"[..]), true),
            ("krabka-log-timestamp-ns", Some(&b"1"[..]), true),
            ("krabka-log-label-app", Some(&b"api"[..]), true),
            ("krabka-wal-record-type", Some(&b"log"[..]), false),
            ("other", None, false),
        ] {
            let header = KafkaWalHeader {
                key: key.to_string(),
                value: value.map(<[u8]>::to_vec),
            };
            assert_eq!(has_native_kafka_log_headers(&[header]), want);
        }
    }

    #[test]
    fn varint_encoding_and_ingest_limits_pin_boundaries() {
        let mut body = Vec::new();
        encode_varint(0, &mut body);
        encode_varint(127, &mut body);
        encode_varint(128, &mut body);
        encode_varint(300, &mut body);
        assert_eq!(body, vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);

        let state = DistributorState {
            sink: Arc::new(InMemoryWalSink::default()),
            ingest_limiter: Arc::new(AllowAllIngestLimiter),
            prepare_shutdown: Arc::new(AtomicBool::new(false)),
            metrics: ServiceMetrics::new(),
            max_ingest_body: Some(bytes(5)),
            wal_append_timeout: None,
            reject_old_samples_max_age: None,
            creation_grace_period: None,
        };
        assert!(validate_ingest_body_limit(&state, bytes(5)).is_ok());
        assert!(validate_ingest_body_limit(&state, bytes(6)).is_err());
    }

    fn loki_content_type(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, value.parse().unwrap());
        headers
    }

    #[test]
    fn loki_content_type_and_body_decoding_accept_only_expected_forms() {
        let mut headers = HeaderMap::new();
        assert_eq!(decode_loki_http_body(&headers, b"raw").unwrap(), b"raw");
        headers.insert(CONTENT_ENCODING, "snappy".parse().unwrap());
        assert_eq!(decode_loki_http_body(&headers, b"raw").unwrap(), b"raw");
        headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, b"raw").unwrap();
        assert_eq!(
            decode_loki_http_body(&headers, &encoder.finish().unwrap()).unwrap(),
            b"raw"
        );
        headers.insert(CONTENT_ENCODING, "br".parse().unwrap());
        assert!(decode_loki_http_body(&headers, b"raw").is_err());

        for (value, want) in [
            ("application/json", Some(true)),
            ("Application/JSON; charset=utf-8", Some(true)),
            ("application/x-protobuf", Some(false)),
            ("application/json; charset", None),
            ("application/json; charset=", None),
        ] {
            assert_eq!(
                is_loki_json_content_type(&loki_content_type(value)).ok(),
                want
            );
        }
    }

    #[test]
    fn loki_error_contexts_respect_utf8_boundaries_and_offsets() {
        let body = "{\"streams\":\"not-array\"}";
        assert!(
            loki_json_push_streams_parse_error(body.as_bytes(), &json!("not-array"))
                .contains("|{\"streams\":\"not-array\"}|")
        );
        // That assertion reads the *bigger* context, which is the whole body
        // either way. The narrow window is twenty bytes from nine before the
        // value, and stops short of the closing brace.
        check!(
            loki_json_push_streams_parse_error(body.as_bytes(), &json!("not-array"))
                .contains(r#"...|streams":"not-array"|..."#),
            "the narrow window is twenty bytes wide"
        );

        // The payload error's window is eleven bytes from the first
        // non-whitespace byte. A body that starts with one puts that at zero,
        // which is the only offset where a width computed by multiplying
        // rather than adding gives a different answer.
        check!(
            loki_json_push_payload_parse_error(b"\"not-json-at-all\"")
                .contains(r#"...|"not-json-a|..."#),
            "eleven bytes from the start"
        );

        let structured =
            br#"{"streams":[{"stream":{"app":"api"},"values":[["1","line",{"ok":true}]]}]}"#;
        let error = loki_structured_metadata_value_parse_error(structured, "ok", &json!(true));
        // The context window starts three bytes before the VALUE, which sits
        // one past the quoted key and its colon. A `contains` on the key and
        // value together is satisfied by any nearby offset -- the whole
        // eighty-byte window holds them -- so the window is pinned exactly.
        check!(
            error.contains(r#"...|k":true}]]}]}|..."#),
            "the context starts three bytes into the value: {error}"
        );

        let text = "ab\u{20ac}cd";
        assert_eq!(previous_char_boundary(text, 4), 2);
        assert_eq!(previous_char_boundary(text, text.len()), text.len());
    }

    #[test]
    fn loki_label_and_level_helpers_pin_boundaries() {
        let rendered_labels = BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]);
        assert_eq!(
            loki_label_set(&rendered_labels),
            r#"{app="api",env="prod"}"#
        );
        check!(loki_push_label_parse_error(&rendered_labels, "bad-name").contains("1:5"));
        // Every character of "bad-name" is judged the same way whether or not
        // it is treated as the first, so that case cannot tell the two apart.
        // A digit can: it is allowed anywhere except at the start.
        let digit_then_invalid = loki_push_label_parse_error(&rendered_labels, "b9-name");
        check!(
            digit_then_invalid.contains("1:4"),
            "the hyphen is the third character: {digit_then_invalid}"
        );
        check!(
            digit_then_invalid.contains("'-'"),
            "and the hyphen is what is reported: {digit_then_invalid}"
        );
        check!(
            loki_proto_label_parse_error(r#"{9bad="x"}"#)
                .unwrap()
                .contains("1:2")
        );
        check!(
            loki_proto_label_parse_error(r#"{app="api",9bad="x"}"#)
                .unwrap()
                .contains("1:12")
        );
        // A digit is fine once a name has started. Both cases above are
        // rejections, so without this the tracking could judge every character
        // by the first one's rule and they would still pass.
        check!(loki_proto_label_parse_error(r#"{a9="x"}"#).is_none());
        check!(loki_proto_label_parse_error(r#"{app="api",b9="x"}"#).is_none());

        // A comma starts a new name even when no `=` came between: in
        // `{app="api",...}` the `=` has already reset the tracking, so only a
        // list without values shows the comma doing it.
        check!(
            loki_proto_label_parse_error("{app,9bad}")
                .unwrap()
                .contains("1:6")
        );
        check!(loki_proto_label_parse_error("{app,b9}").is_none());
        // After `=` the parser stops looking for a name, so an unquoted value
        // is not judged as one. A quoted value never shows this: the string
        // handling swallows it before the name check is reached.
        check!(loki_proto_label_parse_error("{app=bad-value}").is_none());

        let mut detected = BTreeMap::from([("app".to_string(), "api".to_string())]);
        discover_detected_level_label(&mut detected, "api ERROR happened");
        assert_eq!(
            detected.get("detected_level").map(String::as_str),
            Some("error")
        );
        // Any one of the four labels already present stops the discovery, and
        // each has to be the ONLY one present -- a guard that needed two of
        // them would still be stopped by a pair.
        for held in ["detected_level", "level", "severity", "severity_text"] {
            let mut labels = BTreeMap::from([(held.to_string(), "custom".to_string())]);
            discover_detected_level_label(&mut labels, "api error happened");
            check!(
                labels.get("detected_level").map(String::as_str)
                    == if held == "detected_level" {
                        Some("custom")
                    } else {
                        None
                    },
                "{held} alone stops the discovery"
            );
        }
        for (line, want) in [
            ("error happened", true),
            ("happened error", true),
            ("terror", false),
            ("error_code", false),
        ] {
            assert_eq!(contains_log_level_token(line, "error"), want);
        }
        for (byte, want) in [(b'a', true), (b'1', true), (b'_', true), (b'-', false)] {
            assert_eq!(is_log_level_word_byte(byte), want);
        }
    }

    /// `remove_empty_object_field` drops a field only when it is an object
    /// with nothing in it. A field that holds something stays, and so does one
    /// that is not an object at all -- an empty array is not an empty object.
    #[test]
    fn an_empty_object_field_is_removed_and_nothing_else_is() {
        let mut value = serde_json::json!({
            "empty": {},
            "full": {"a": 1},
            "array": [],
            "null": null,
        });
        for field in ["empty", "full", "array", "null"] {
            super::remove_empty_object_field(&mut value, field);
        }
        check!(
            value == serde_json::json!({"full": {"a": 1}, "array": [], "null": null}),
            "got {value}"
        );

        // A value that is not an object at all is left alone rather than
        // panicking on the way past.
        let mut scalar = serde_json::json!(7);
        super::remove_empty_object_field(&mut scalar, "empty");
        check!(scalar == serde_json::json!(7));
    }

    /// The three `LogQL` set operators each map to their own variant, and an
    /// unknown word maps to none. Deleting an arm does not fail to compile --
    /// it falls to the catch-all and the operator simply stops existing.
    #[test]
    fn every_metric_set_operator_maps_to_its_own_variant() {
        use super::MetricBinarySetOp;

        check!(super::parse_metric_set_operator("and") == Some(MetricBinarySetOp::And));
        check!(super::parse_metric_set_operator("or") == Some(MetricBinarySetOp::Or));
        check!(super::parse_metric_set_operator("unless") == Some(MetricBinarySetOp::Unless));
        check!(super::parse_metric_set_operator("nor") == None);
        check!(super::parse_metric_set_operator("") == None);
    }

    #[test]
    fn timestamp_and_value_conversions_cover_json_and_proto_shapes() {
        assert_eq!(otlp_timestamp_ns(&json!("123")).unwrap(), 123);
        assert_eq!(otlp_timestamp_ns(&json!(456)).unwrap(), 456);
        assert!(otlp_timestamp_ns(&json!(-1)).is_err());
        assert_eq!(
            otlp_severity_number_to_string(&json!("INFO")).unwrap(),
            "INFO"
        );
        assert_eq!(otlp_severity_number_to_string(&json!(9)).unwrap(), "9");

        let otlp_value = OtlpAnyValue::Kvlist(OtlpKeyValueList {
            values: Some(vec![
                OtlpKeyValue {
                    key: "ok".to_string(),
                    value: OtlpAnyValue::Bool(true),
                },
                OtlpKeyValue {
                    key: "items".to_string(),
                    value: OtlpAnyValue::Array(OtlpArrayValue {
                        values: Some(vec![OtlpAnyValue::String("a".to_string())]),
                    }),
                },
            ]),
        });
        assert_eq!(
            otlp_value_to_json(&otlp_value),
            json!({"items": ["a"], "ok": true})
        );

        let proto_value = ProtoAnyValue {
            value: Some(proto_any_value::Value::KvlistValue(
                opentelemetry_proto::tonic::common::v1::KeyValueList {
                    values: vec![ProtoKeyValue {
                        key: "answer".to_string(),
                        value: Some(ProtoAnyValue {
                            value: Some(proto_any_value::Value::IntValue(42)),
                        }),
                        ..Default::default()
                    }],
                },
            )),
        };
        assert_eq!(proto_value_to_json(&proto_value), json!({"answer": 42}));
    }

    #[test]
    fn delete_request_query_parsing_and_overlap_boundaries() {
        let params = parse_create_delete_request_params(Some(
            "query=%7Bapp%3D%22api%22%7D&start=10&end=20&max_interval=1h",
        ))
        .unwrap();
        assert_eq!(params.query, r#"{app="api"}"#);
        assert_eq!(params.start_time, 10);
        assert_eq!(params.end_time, 20);
        assert!(parse_create_delete_request_params(Some("query=x&start=20&end=10")).is_err());
        // A window of zero width is allowed: "end before start" is the error,
        // not "end not after start".
        check!(
            parse_create_delete_request_params(Some("query=x&start=10&end=10")).is_ok(),
            "a start and end at the same instant"
        );
        // `max_interval` is parsed for its own sake -- the value is discarded,
        // so only an invalid one shows the parse happening at all. The case
        // above passes `1h`, which is accepted whether or not it is read.
        check!(
            parse_create_delete_request_params(Some(
                "query=x&start=10&end=20&max_interval=notaduration"
            ))
            .is_err(),
            "an unparseable max_interval is refused"
        );

        let list = parse_list_delete_requests_params(Some("start=10&end=20")).unwrap();
        assert_eq!(list.start_time, Some(10));
        assert_eq!(list.end_time, Some(20));
        assert!(parse_list_delete_requests_params(Some("start=10")).is_err());
        assert_eq!(
            parse_cancel_delete_request_params(Some("request_id=delete-1&force=true")).unwrap(),
            "delete-1"
        );
        assert!(
            parse_cancel_delete_request_params(Some("request_id=delete-1&force=maybe")).is_err()
        );
        assert_eq!(
            parse_loki_delete_timestamp_query_param("start", "1.5").unwrap(),
            1
        );

        let request = CompactorDeleteRequest {
            tenant: "tenant-a".to_string(),
            request_id: "delete-1".to_string(),
            query: r#"{app="api"}"#.to_string(),
            start_time: 10,
            end_time: 20,
            status: "received".to_string(),
            created_at: 1,
        };
        for (filter, want) in [
            (list, true),
            (
                ListDeleteRequestsParams {
                    start_time: Some(20),
                    end_time: Some(30),
                },
                true,
            ),
            (
                ListDeleteRequestsParams {
                    start_time: Some(21),
                    end_time: Some(30),
                },
                false,
            ),
        ] {
            assert_eq!(delete_request_overlaps_filter(&request, &filter), want);
        }
        for (right, want) in [
            (TimeRange::new(20, 30).unwrap(), true),
            (TimeRange::new(21, 30).unwrap(), false),
        ] {
            assert_eq!(ranges_overlap(TimeRange::new(10, 20).unwrap(), right), want);
        }
    }

    /// Cancelling a delete request removes the one request that matches BOTH
    /// the tenant and the id. Two tenants can hold the same id -- the counter
    /// is per store, but the ids are handed out per tenant view -- so a cancel
    /// that matched on either alone would take a request belonging to someone
    /// else.
    #[test]
    fn cancelling_a_delete_request_takes_only_that_tenant_s() {
        let request = |tenant: &str, request_id: &str| super::CompactorDeleteRequest {
            tenant: tenant.to_string(),
            request_id: request_id.to_string(),
            query: r#"{app="api"}"#.to_string(),
            start_time: 0,
            end_time: 1,
            status: "received".to_string(),
            created_at: 0,
        };
        let state = super::CompactorDeleteState {
            delete_requests: super::SharedLogDeleteRequests::default(),
        };
        state
            .delete_requests
            .inner
            .lock()
            .expect("the delete state lock is held")
            .requests = vec![
            request("tenant-a", "delete-1"),
            request("tenant-b", "delete-1"),
            request("tenant-a", "delete-2"),
        ];

        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
        super::execute_cancel_delete_request(&state, &headers, Some("request_id=delete-1"))
            .expect("the cancel succeeds");

        let left = state
            .delete_requests
            .inner
            .lock()
            .expect("the delete state lock is held")
            .requests
            .iter()
            .map(|request| (request.tenant.clone(), request.request_id.clone()))
            .collect::<Vec<_>>();
        check!(
            left == vec![
                ("tenant-b".to_string(), "delete-1".to_string()),
                ("tenant-a".to_string(), "delete-2".to_string()),
            ],
            "got {left:?}"
        );
    }

    /// The four POST query endpoints each answer with a JSON body. Replacing
    /// any of them with a default `Response` yields an empty 200 -- a status
    /// check alone accepts that, so the body has to be read.
    #[tokio::test]
    async fn the_post_query_endpoints_answer_with_a_body() {
        use axum::{extract::State, response::IntoResponse as _};

        let dir = tempfile::TempDir::new().expect("temp dir");
        let state = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default());
        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
        let body = || {
            axum::body::Bytes::from_static(b"query=%7Bapp%3D%22web%22%7D&start=0&end=1000000000")
        };
        let read = |response: axum::response::Response| async move {
            let status = response.status();
            let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
                .await
                .expect("the response body is readable");
            (status, bytes)
        };

        for (name, response) in [
            (
                "detected_fields",
                super::detected_fields_post(
                    State(state.clone()),
                    headers.clone(),
                    axum::extract::RawQuery(None),
                    body(),
                )
                .await,
            ),
            (
                "detected_labels",
                super::detected_labels_post(
                    State(state.clone()),
                    headers.clone(),
                    axum::extract::RawQuery(None),
                    body(),
                )
                .await,
            ),
            (
                "index_volume",
                super::index_volume_post(
                    State(state.clone()),
                    headers.clone(),
                    axum::extract::RawQuery(None),
                    body(),
                )
                .await,
            ),
            (
                "label_names",
                super::api_prom_label_names_post(
                    State(state.clone()),
                    headers.clone(),
                    axum::extract::RawQuery(None),
                    body(),
                )
                .await,
            ),
        ] {
            let (status, bytes) = read(response.into_response()).await;
            check!(status == axum::http::StatusCode::OK, "{name}: {status}");
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .unwrap_or_else(|error| panic!("{name}: body is not JSON ({error}): {bytes:?}"));
            if name == "index_volume" {
                check!(
                    value["status"] == "success" && value["data"]["resultType"] == "vector",
                    "{name}: got {value}"
                );
                check!(
                    value["data"]["result"].as_array().map(Vec::len) == Some(0),
                    "an empty store has no volume series: {value}"
                );
            } else {
                // An empty store answers with an empty JSON object, not with an
                // empty body -- a client parsing the response needs something
                // to parse.
                check!(value == serde_json::json!({}), "{name}: got {value}");
            }
        }
    }

    /// The patterns scan drops a row outside the query window, and the window
    /// is half-open: a row exactly on the start counts, one exactly on the end
    /// does not. Nothing had scanned a block through this endpoint, so the two
    /// edges and the `||` joining them to the fingerprint test were all free.
    #[tokio::test]
    async fn a_patterns_scan_keeps_the_window_half_open() {
        use krabka_blockstore::{BlockKey, LogRow, TimeRange, series_fingerprint, write_log_block};

        let dir = tempfile::tempdir().expect("a temp dir");
        let mut labels = Labels::new();
        labels.insert("app".to_string(), "web".to_string());
        let fingerprint = series_fingerprint(&labels);

        let row = |timestamp_ns, line: &str| LogRow {
            series_fingerprint: fingerprint,
            timestamp_ns,
            line: line.to_string(),
            structured_metadata: BTreeMap::new(),
        };
        // Before the window, on its start, inside, on its end, and past it.
        // The rows outside carry a different line shape, so including one
        // shows up as a second pattern rather than merely a larger count --
        // swapping which rows are kept leaves the count alone.
        let key = BlockKey::new(
            "tenant-a",
            0,
            0,
            0,
            TimeRange::new(0, 100).expect("a valid range"),
        );
        let descriptor = write_log_block(
            dir.path(),
            &key,
            vec![
                row(5, "cache warmed"),
                row(10, "request served"),
                row(20, "request served"),
                row(30, "cache warmed"),
                row(40, "cache warmed"),
            ],
        )
        .expect("the block writes");

        let mut index = BlockIndex::default();
        index.insert(descriptor);
        let mut label_index = LabelIndex::default();
        label_index.insert_series("tenant-a", labels);
        let state = QuerierState::new(dir.path(), label_index, index);

        let mut headers = HeaderMap::new();
        headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
        let value = super::execute_patterns_query(
            &state,
            &headers,
            Some("query=%7Bapp%3D%22web%22%7D&start=10&end=30&step=1h"),
        )
        .await
        .expect("the patterns query runs");

        // One line shape, one bucket, and only the two rows inside the window.
        let data = value["data"].as_array().expect("a data array");
        check!(data.len() == 1, "one pattern: {value}");
        let samples = data[0]["samples"].as_array().expect("a samples array");
        check!(samples.len() == 1, "one bucket: {value}");
        check!(
            samples[0][1] == 2,
            "the row on the start counts and the one on the end does not: {value}"
        );
    }

    #[test]
    fn prometheus_rules_filters_parse_all_supported_axes() {
        let filters = PrometheusRulesFilters::parse(Some(
            "type=alert&exclude_alerts=true&time=10&rule_name=HighError&rule_group=api&file=rules.yaml&group_limit=2&group_next_token=next&match=%7Bapp%3D%22api%22%7D",
        ))
        .unwrap();
        assert_eq!(filters.rule_kind, Some("alerting"));
        check!(filters.exclude_alerts);
        check!(filters.evaluation_time.is_some());
        check!(filters.rule_names.contains("HighError"));
        check!(filters.rule_groups.contains("api"));
        check!(filters.files.contains("rules.yaml"));
        assert_eq!(filters.group_limit, Some(2));
        assert_eq!(filters.group_next_token.as_deref(), Some("next"));
        assert_eq!(filters.label_selectors.len(), 1);
        assert!(filters.has_rule_filter());

        let recording = PrometheusRulesFilters::parse(Some("type=record")).unwrap();
        assert_eq!(recording.rule_kind, Some("recording"));
        assert!(PrometheusRulesFilters::parse(Some("group_next_token=next")).is_err());
        assert!(
            !PrometheusRulesFilters::parse(Some(""))
                .unwrap()
                .has_rule_filter()
        );
    }

    #[test]
    fn json_log_lines_collapse_to_a_single_templated_pattern() {
        // Two Krabka-shaped JSON log lines differing only by timestamp must mine
        // to one pattern with the timestamp templatized and every constant kept.
        let first = r#"{"timestamp":"2026-07-01T04:19:26.1238077Z","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#;
        let second = r#"{"timestamp":"2026-07-01T04:19:27.9981001Z","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#;
        assert_eq!(log_line_pattern(first), log_line_pattern(second));
        assert_eq!(
            log_line_pattern(first),
            r#"{"timestamp":"<_>","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#
        );
    }

    #[test]
    fn json_log_pattern_templatizes_ids_and_numbers_but_keeps_constants() {
        let pattern = log_line_pattern(
            r#"{"severity":"INFO","request_id":"550e8400-e29b-41d4-a716-446655440000","trace":"4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b","offset":12345,"sasl":false,"listener":"PLAIN"}"#,
        );
        assert_eq!(
            pattern,
            r#"{"severity":"INFO","request_id":"<_>","trace":"<_>","offset":"<_>","sasl":false,"listener":"PLAIN"}"#
        );
    }

    #[test]
    fn json_message_field_templatizes_embedded_variables() {
        assert_eq!(
            log_line_pattern(r#"{"message":"processed request 550e8400e29b41d4a716 in 42ms"}"#),
            r#"{"message":"processed request <_> in <_>"}"#
        );
    }

    #[test]
    fn non_json_lines_still_use_logfmt_mining() {
        assert_eq!(
            log_line_pattern("status=500 user=100 route=/checkout"),
            "status=<_> user=<_> route=/checkout"
        );
        // A line that merely starts with `{` but is not valid JSON falls back.
        assert_eq!(log_line_pattern("{not json ts=1"), "{not json ts=<_>");
    }

    #[test]
    fn pattern_value_variable_classification() {
        // Variable: timestamps, floats, UUIDs, long hex ids, opaque tokens.
        assert!(pattern_value_is_variable("2026-07-01T04:19:26.1238077Z"));
        assert!(pattern_value_is_variable("42.5"));
        assert!(pattern_value_is_variable(
            "550e8400-e29b-41d4-a716-446655440000"
        ));
        assert!(pattern_value_is_variable(
            "4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b"
        ));
        assert!(pattern_value_is_variable("AKIAIOSFODNN7EXAMPLE"));
        assert!(pattern_value_is_variable("\"2026-07-01T04:19:26Z\""));
        // Sole-reason coverage: each value below is variable via exactly one
        // classifier, so every branch of the `||` chain (and the shape checks
        // inside `is_uuid`/`is_hex_id`) is independently exercised.
        assert!(pattern_value_is_variable("-42.5")); // negative float: only the f64 parse
        assert!(pattern_value_is_variable(
            "f47ac10b-58cc-4372-a567-0e02b2c3d479" // letter-led UUID: only is_uuid
        ));
        assert!(pattern_value_is_variable("abcdefabcdefabcd")); // 16 hex letters, no digit: only is_hex_id
        // UUID *layout* but non-hex groups must not be accepted as a UUID (guards
        // the `len == n && all-hex` check inside is_uuid).
        assert!(!pattern_value_is_variable(
            "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
        ));
        // Constant: levels, module paths, file:line callers, short words.
        assert!(!pattern_value_is_variable("INFO"));
        assert!(!pattern_value_is_variable(
            "krabka_broker::network::dispatch"
        ));
        assert!(!pattern_value_is_variable("grpc_logging.go:66"));
        assert!(!pattern_value_is_variable("/cortex.Ingester/Push"));
        assert!(!pattern_value_is_variable("cafe"));
        assert!(!pattern_value_is_variable("authenticationToken"));
    }

    /// A negative range offset MUST render with a leading `-` sign, and a positive
    /// offset MUST NOT. This pins the `offset_ns.0 < 0` sign branch in
    /// `format_metric_range_selector`. A replacement of `<` with `==` would
    /// drop the sign and emit a positive offset for a query that asked to look
    /// *forward* in time. That `==` is never true here, because the outer guard
    /// handles the `== 0` case.
    #[test]
    fn format_metric_range_selector_signs_negative_offset() {
        let negative = parse_metric_query("count_over_time({app=\"x\"}[5m] offset -3m)").unwrap();
        let positive = parse_metric_query("count_over_time({app=\"x\"}[5m] offset 3m)").unwrap();

        let negative_selector =
            format_metric_range_selector(&negative).expect("negative offset selector");
        let positive_selector =
            format_metric_range_selector(&positive).expect("positive offset selector");

        // The negative offset carries the sign; the positive one does not.
        check!(negative_selector.contains(" offset -"));
        check!(!positive_selector.contains(" offset -"));
        // The two differ ONLY by the sign character.
        check!(negative_selector == positive_selector.replace(" offset ", " offset -"));
    }

    /// `count_loki_metric_result_hot_tail_samples` counts matched ingester samples and
    /// returns 0 when there is nothing to match: an `absent_over_time` query short-
    /// circuits to 0, and a query whose response JSON has no `data.result`
    /// array also yields 0. A replacement of the whole body with a constant
    /// `1`, the mutant, would report a phantom ingester sample and skew the
    /// store/ingester scan-stat split.
    #[test]
    fn count_loki_metric_result_hot_tail_samples_returns_zero_when_nothing_matches() {
        let plan = StreamPlan {
            tenant: "tenant".to_string(),
            time_range: TimeRange::new(0, 300_000_000_000).unwrap(),
            query: StreamQuery {
                matchers: Vec::new(),
                pipeline: Vec::new(),
            },
            fingerprints: BTreeSet::new(),
            blocks: Vec::new(),
        };
        let frontier = CompactionFrontier::new(0);
        let eval_range = TimeRange::new(0, 300_000_000_000).unwrap();
        let step_ns = 60_000_000_000;

        // `absent_over_time` short-circuits to 0 regardless of the response body.
        let absent_query = parse_metric_query("absent_over_time({app=\"x\"}[5m])").unwrap();
        let absent = count_loki_metric_result_hot_tail_samples(
            &json!({ "data": { "result": [] } }),
            &plan,
            &absent_query,
            &[],
            &frontier,
            (eval_range, step_ns),
            &[],
        );
        check!(absent == 0);

        // A non-absent query with an empty hot tail and a response lacking any
        // `data.result` array matches nothing and returns 0.
        let count_query = parse_metric_query("count_over_time({app=\"x\"}[5m])").unwrap();
        let none = count_loki_metric_result_hot_tail_samples(
            &json!({}),
            &plan,
            &count_query,
            &[],
            &frontier,
            (eval_range, step_ns),
            &[],
        );
        check!(none == 0);
    }
}
