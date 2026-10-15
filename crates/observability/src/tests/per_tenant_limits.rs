use crate::tests::prelude::{
    Arc, BlockIndex, ByteSize, ConfiguredObjectStore, DistributorError, HttpQueryError, LabelIndex,
    Labels, Limits, ObjectPath, OverridesError, OverridesProvider, QuerierIndexSource,
    QuerierState, RetentionWindows, ServiceConfig, TenantId, Time, TimeExt, TimeRange,
    build_configured_querier_state, bytes, check, clamp_query_lookback, days, hours,
    limits_for_config, minutes, secs, validate_ingest_body_limit, validate_loki_label_limits,
    validate_loki_line_size, validate_query_entries_limit,
};

mod the_overrides_file_round_trips_through_the_limit_set {
use super::*;
use crate::{OtlpAttributeAction, OtlpAttributesConfig, OtlpConfig, OtlpResourceAttributesConfig};

/// Every field of the limit set has to survive a write and a read. A field
/// whose serde attribute names the wrong unit adapter, or whose key differs
/// between the full struct and the sparse override struct, would load as a
/// default and the operator would never see it refused.
///
/// The round trip goes through the sparse override struct, which is the one
/// an operator's file is read into, so the two structs are checked against
/// each other rather than each against itself.
#[test]
pub(crate) fn the_overrides_file_round_trips_through_the_limit_set() {
    let limits = Limits {
        query_admission: krabka_query_frontend::AdmissionLimits::default(),
        max_line_size: bytes(4096),
        max_line_size_truncate: true,
        max_structured_metadata_size: bytes(3072),
        max_structured_metadata_entries_count: 6,
        discover_log_levels: false,
        log_level_fields: vec!["priority".into(), "LogPriority".into()],
        log_level_from_json_max_depth: -1,
        otlp_config: OtlpConfig {
            resource_attributes: OtlpResourceAttributesConfig {
                ignore_defaults: true,
                attributes_config: vec![OtlpAttributesConfig {
                    action: OtlpAttributeAction::IndexLabel,
                    attributes: vec!["host.name".into()],
                    regex: None,
                }],
            },
            scope_attributes: vec![OtlpAttributesConfig {
                action: OtlpAttributeAction::Drop,
                attributes: vec!["scope.secret".into()],
                regex: None,
            }],
            log_attributes: vec![OtlpAttributesConfig {
                action: OtlpAttributeAction::Drop,
                attributes: vec!["log.secret".into()],
                regex: None,
            }],
        },
        max_label_names_per_series: 7,
        max_label_name_length: bytes(64),
        max_label_value_length: bytes(128),
        reject_old_samples_max_age: days(3),
        creation_grace_period: minutes(4),
        max_ingest_body: bytes(2048),
        max_query_length: hours(5),
        max_query_lookback: days(6),
        max_entries_limit_per_query: 11,
        max_query_series: 12,
        enable_multi_variant_queries: true,
        shard_aggregations: vec!["approx_topk".into()],
        max_query_read: bytes(8192),
        max_query_string_bytes: bytes(256),
        max_query_range: secs(13),
        retention_period: days(14),
    };

    // `Limits` serialises to exactly the keys the override struct reads, so
    // one tenant's whole set can be written back out as its override.
    let body = serde_yaml::to_string(&limits).expect("the limit set serialises");
    let yaml = String::from("overrides:\n  tenant-a:\n")
        + &body
            .lines()
            .map(|line| String::from("    ") + line)
            .collect::<Vec<_>>()
            .join("\n");

    let provider = OverridesProvider::from_yaml(&yaml).expect("the written file parses");
    check!(*provider.for_tenant(&TenantId::new("tenant-a").expect("a valid tenant id")) == limits);

    // And the `defaults` block reads the same keys.
    let yaml = yaml.replace("overrides:\n  tenant-a:\n", "defaults:\n");
    let provider = OverridesProvider::from_yaml(&yaml).expect("the defaults block parses");
    check!(*provider.defaults() == limits);
}
}
mod the_scalar_limit_flags_become_the_provider_defaults;

/// A key an operator misspells must fail the load. Ignoring it would leave
/// them believing a limit was set, and the first they would hear of it is a
/// tenant that was never capped.
///
/// A negative value must fail too. Zero is the one sentinel for "no limit",
/// and a negative cap would read as another one: every check here applies
/// only a value greater than zero.
#[test]
pub(crate) fn a_misspelled_or_negative_override_is_refused_rather_than_ignored() {
    let refused = [
        // A misspelled key in a tenant entry, in the defaults block, and at
        // the top level.
        "overrides:\n  tenant-a:\n    max_query_serie: 3\n",
        "defaults:\n  max_line_sizes: \"1KiB\"\n",
        "overrides: {}\nbogus_top_level: true\n",
        // A negative time cap and a negative size cap.
        "overrides:\n  tenant-a:\n    max_query_length: \"-1s\"\n",
        "defaults:\n  max_query_lookback: \"-1h\"\n",
        "overrides:\n  tenant-a:\n    max_line_size: \"-1B\"\n",
        // A dimensioned value with no unit is not guessed at.
        "overrides:\n  tenant-a:\n    max_query_length: 30\n",
    ];
    for yaml in refused {
        let error = OverridesProvider::from_yaml(yaml).expect_err(yaml);
        check!(matches!(error, OverridesError::Yaml(_)), "{yaml}: {error}");
    }

    // Zero is accepted on both sides, and it is what turns a cap off.
    let provider = OverridesProvider::from_yaml(
        "overrides:\n  tenant-a:\n    max_query_length: \"0s\"\n    max_line_size: \"0B\"\n",
    )
    .expect("zero turns a cap off");
    check!(
        provider
            .for_tenant(&TenantId::new("tenant-a").expect("a valid tenant id"))
            .max_query_length
            == Time::ZERO
    );
    check!(
        provider
            .for_tenant(&TenantId::new("tenant-a").expect("a valid tenant id"))
            .max_line_size
            == bytes(0)
    );
}

/// The four ingest-side caps share a shape: zero means no limit, a value
/// exactly at the limit is accepted, and one byte or one name over is
/// refused. Each is checked at all three points, because `>` and `>=`
/// differ only at the boundary.
///
/// The refusals are checked by variant rather than by `is_err`, because all
/// four produce a `DistributorError` and a mutant that swapped one check's
/// error for another's would otherwise survive.
#[test]
pub(crate) fn every_new_ingest_limit_admits_exactly_its_boundary() {
    let stream = |names: usize, value: &str| {
        let mut labels = Labels::default();
        for index in 0..names {
            labels.insert(format!("label_{index}"), value.to_string());
        }
        labels
    };
    let capped = |limits: Limits| limits;

    // Line size, measured in UTF-8 bytes of the line.
    let labels = stream(1, "v");
    let line = "0123456789";
    let line_limit = |max_line_size: ByteSize| {
        capped(Limits {
            max_line_size,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_line_size(line, &labels, &Limits::unenforced()).is_ok(),
        "off"
    );
    check!(
        validate_loki_line_size(line, &labels, &line_limit(bytes(10))).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_line_size(line, &labels, &line_limit(bytes(9))),
        Err(DistributorError::LineTooLong { .. })
    ));

    // Label count.
    let count_limit = |max_label_names_per_series: u64| {
        capped(Limits {
            max_label_names_per_series,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_label_limits(&stream(3, "v"), &Limits::unenforced()).is_ok(),
        "off"
    );
    check!(
        validate_loki_label_limits(&stream(3, "v"), &count_limit(3)).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_label_limits(&stream(3, "v"), &count_limit(2)),
        Err(DistributorError::TooManyLabelNames { .. })
    ));

    // Label name length. "label_0" is seven bytes.
    let name_limit = |max_label_name_length: ByteSize| {
        capped(Limits {
            max_label_name_length,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_label_limits(&stream(1, "v"), &name_limit(bytes(7))).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_label_limits(&stream(1, "v"), &name_limit(bytes(6))),
        Err(DistributorError::LabelNameTooLong { .. })
    ));

    // Label value length.
    let value_limit = |max_label_value_length: ByteSize| {
        capped(Limits {
            max_label_value_length,
            ..Limits::unenforced()
        })
    };
    check!(
        validate_loki_label_limits(&stream(1, "abcd"), &value_limit(bytes(4))).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_loki_label_limits(&stream(1, "abcd"), &value_limit(bytes(3))),
        Err(DistributorError::LabelValueTooLong { .. })
    ));

    // The ingest body cap, which is Krabka's own.
    let body_limit = Limits {
        max_ingest_body: bytes(100),
        ..Limits::unenforced()
    };
    check!(validate_ingest_body_limit(&body_limit, bytes(100)).is_ok());
    check!(matches!(
        validate_ingest_body_limit(&body_limit, bytes(101)),
        Err(DistributorError::IngestBodyTooLarge { .. })
    ));

    // Each refusal names the stream and the numbers a client needs.
    let error = validate_loki_line_size(line, &labels, &line_limit(bytes(9)))
        .expect_err("one byte over the line cap");
    let message = error.to_string();
    check!(message.contains("Max entry size '9'"), "{message}");
    check!(message.contains("length '10' bytes"), "{message}");
}

/// `max_entries_limit_per_query` caps the `limit` parameter a client asks
/// for. A request that names no `limit` is not checked at all, which is a
/// third answer distinct from a `limit` of zero, and zero on the limit side
/// turns the cap off.
#[test]
pub(crate) fn the_entries_limit_admits_exactly_the_count_it_names() {
    let state = |max_entries_limit_per_query: u64| {
        QuerierState::new(".", LabelIndex::default(), BlockIndex::default()).with_limits(Limits {
            max_entries_limit_per_query,
            ..Limits::unenforced()
        })
    };

    check!(
        validate_query_entries_limit(&state(0), Some(1_000_000)).is_ok(),
        "zero turns the cap off"
    );
    check!(
        validate_query_entries_limit(&state(10), None).is_ok(),
        "a request that names no limit is not checked"
    );
    check!(
        validate_query_entries_limit(&state(10), Some(10)).is_ok(),
        "exactly at the limit"
    );
    check!(matches!(
        validate_query_entries_limit(&state(10), Some(11)),
        Err(HttpQueryError::MaxEntriesLimitPerQuery { .. })
    ));

    // The message is Loki's own, and it names both numbers in Loki's order.
    let error = validate_query_entries_limit(&state(10), Some(11)).expect_err("one over");
    check!(
        error.to_string()
            == "max entries limit per query exceeded, limit > max_entries_limit_per_query (11 > 10)",
        "{error}"
    );
}

/// `max_query_lookback` clamps rather than refuses, which is what `Loki`
/// does. A query that starts before the boundary is answered from the
/// boundary forward, and a query that starts after it is left alone.
///
/// The end is never moved backwards. A clamp that moved the end would turn
/// a legitimate query into an empty one, and the caller would see a
/// successful response with no rows and no reason.
#[test]
pub(crate) fn the_lookback_clamp_moves_the_start_and_leaves_the_end() {
    let now_ns = 1_000_000_000_000_000_i64;
    let hour_ns = hours(1).nanos_i64();
    let limits = |max_query_lookback| Limits {
        max_query_lookback,
        ..Limits::unenforced()
    };
    let range = |start_ns, end_ns| TimeRange::new(start_ns, end_ns).expect("a valid range");

    // Off: the range is handed back untouched.
    let asked = range(0, now_ns);
    check!(clamp_query_lookback(&limits(Time::ZERO), asked, now_ns) == asked);

    // Wholly inside the window: untouched.
    let inside = range(now_ns - hour_ns / 2, now_ns);
    check!(clamp_query_lookback(&limits(hours(1)), inside, now_ns) == inside);

    // Exactly at the boundary: untouched, because the boundary instant is
    // itself within the lookback.
    let boundary = range(now_ns - hour_ns, now_ns);
    check!(clamp_query_lookback(&limits(hours(1)), boundary, now_ns) == boundary);

    // Starting before the boundary: the start moves, the end does not.
    let clamped = clamp_query_lookback(&limits(hours(1)), range(0, now_ns), now_ns);
    check!(clamped.start_ns == now_ns - hour_ns);
    check!(clamped.end_ns == now_ns, "the end is never moved");

    // Wholly before the boundary: the whole window collapses onto the
    // boundary instant, so the query reads nothing the lookback forbids.
    let outside = clamp_query_lookback(&limits(hours(1)), range(0, now_ns - 2 * hour_ns), now_ns);
    check!(outside.start_ns == now_ns - hour_ns);
    check!(outside.end_ns == now_ns - hour_ns);
}

/// A querier that serves every tenant from a tenant index source takes a
/// branch of its own, because it reads its index per request rather than
/// once at start-up. That branch used to return a bare state, so the
/// deployment that actually serves several tenants was the one deployment
/// with no query limits and no runtime policy at all.
#[tokio::test]
pub(crate) async fn a_tenant_less_dynamic_index_querier_still_carries_its_limits() {
    let configured = ConfiguredObjectStore {
        store: Arc::new(object_store::memory::InMemory::new()),
        prefix: ObjectPath::from("observability"),
    };
    let overrides = Arc::new(
        OverridesProvider::from_yaml_over(
            "overrides:\n  tenant-capped:\n    max_query_series: 3\n",
            &Limits {
                max_query_series: 99,
                ..Limits::default()
            },
        )
        .expect("the overrides file parses"),
    );

    for source in [
        QuerierIndexSource::TenantObjectStoreManifest,
        QuerierIndexSource::TenantObjectStoreShards,
    ] {
        let config = ServiceConfig {
            tenant: None,
            querier_index_source: source,
            index_prefix: Some("index".to_owned()),
            querier_cold_block_fetch_concurrency: std::num::NonZeroUsize::new(3)
                .expect("a nonzero concurrency"),
            ..ServiceConfig::default()
        };
        let state = build_configured_querier_state(&config, &configured, Arc::clone(&overrides))
            .await
            .expect("the configuration is valid");

        check!(state.dynamic_index.is_some(), "{source:?}");
        check!(
            state.limits.max_query_series == 99,
            "the provider's defaults reach this branch: {source:?}"
        );
        check!(
            state
                .with_tenant_limits(&TenantId::new("tenant-capped").expect("a valid tenant id"))
                .limits
                .max_query_series
                == 3,
            "and a per-tenant override resolves through it: {source:?}"
        );
        check!(
            state.cold_block_fetch_concurrency.get() == 3,
            "the runtime policy reaches this branch too: {source:?}"
        );
    }
}

/// An override is sparse by design: the operator writes the one limit they
/// want changed, and the tenant keeps every other value from the defaults.
/// A merge that reset the unnamed fields would silently turn `Loki`'s own
/// defaults off for that tenant, which is the failure this checks for.
///
/// The `defaults` block is checked in the same test, because it merges
/// through the same code and a tenant with no entry of its own must see it.
#[test]
pub(crate) fn a_tenant_override_changes_only_the_limits_it_names() {
    const YAML: &str = r#"
defaults:
  max_line_size: "1KiB"
  discover_log_levels: false
  log_level_fields: [priority, SeverityText]
  log_level_from_json_max_depth: 4
overrides:
  tenant-capped:
    max_query_series: 2
    max_line_size: "16B"
    discover_log_levels: true
    log_level_fields: []
    log_level_from_json_max_depth: 0
  tenant-open:
    max_query_length: "0s"
    log_level_from_json_max_depth: -2
"#;

    let provider = OverridesProvider::from_yaml(YAML).expect("the overrides file parses");

    // The named tenant keeps every limit it did not name.
    let capped = provider.for_tenant(&TenantId::new("tenant-capped").expect("a valid tenant id"));
    check!(capped.max_query_series == 2);
    check!(capped.max_line_size == bytes(16));
    check!(
        capped.max_label_names_per_series == Limits::default().max_label_names_per_series,
        "an unnamed limit keeps Loki's default"
    );
    check!(
        capped.max_query_length == Limits::default().max_query_length,
        "and so does an unnamed time cap"
    );

    check!(
        *capped
            == Limits {
                max_query_series: 2,
                max_line_size: bytes(16),
                discover_log_levels: true,
                log_level_fields: Vec::new(),
                log_level_from_json_max_depth: 0,
                ..Limits::default()
            }
    );

    // A tenant with an entry that names something else still gets the
    // `defaults` block, not the built-in default.
    let open = provider.for_tenant(&TenantId::new("tenant-open").expect("a valid tenant id"));
    check!(open.max_query_length == Time::ZERO, "zero turns a cap off");
    check!(
        open.max_line_size == bytes(1024),
        "the defaults block applies"
    );

    check!(
        *open
            == Limits {
                max_line_size: bytes(1024),
                max_query_length: Time::ZERO,
                discover_log_levels: false,
                log_level_fields: vec!["priority".into(), "SeverityText".into()],
                log_level_from_json_max_depth: -2,
                ..Limits::default()
            }
    );

    // A tenant with no entry at all gets the `defaults` block too.
    let unlisted =
        provider.for_tenant(&TenantId::new("tenant-unlisted").expect("a valid tenant id"));
    check!(unlisted.max_line_size == bytes(1024));
    check!(*unlisted == *provider.defaults());
    check!(
        !provider
            .has_tenant_override(&TenantId::new("tenant-unlisted").expect("a valid tenant id"))
    );
    check!(
        provider.has_tenant_override(&TenantId::new("tenant-capped").expect("a valid tenant id"))
    );
}

struct RetentionCase {
    description: &'static str,
    tenant: &'static str,
    window: Time,
}

/// The sentinel reading, pinned in both directions.
///
/// `Loki` ships `retention_period: 0s`, and zero there means "no retention".
/// Read the other way round it means "delete everything", and the first sweep
/// of a fresh deployment would then delete every block of every tenant that
/// configured nothing. That is not a bug a later fix can undo.
#[test]
pub(crate) fn a_zero_retention_window_keeps_every_block_rather_than_none() {
    // `Loki`'s own default, which is what an operator who writes no file gets.
    check!(Limits::default().retention_period == Time::ZERO);
    check!(!OverridesProvider::new(Limits::default()).expires_any_blocks());

    let provider = OverridesProvider::from_yaml(
        "defaults:\n  retention_period: \"90m\"\noverrides:\n  tenant-short:\n    \
         retention_period: \"1h\"\n  tenant-forever:\n    retention_period: \"0s\"\n",
    )
    .expect("the overrides file parses");

    for case in [
        RetentionCase {
            description: "a tenant with a window of its own",
            tenant: "tenant-short",
            window: hours(1),
        },
        RetentionCase {
            description: "a tenant that turns retention off",
            tenant: "tenant-forever",
            window: Time::ZERO,
        },
        RetentionCase {
            description: "a tenant with no entry at all",
            tenant: "tenant-unlisted",
            window: minutes(90),
        },
    ] {
        check!(
            provider.block_retention(case.tenant) == case.window,
            "{}",
            case.description
        );
    }

    // The sweep costs a pass over the object store, so a deployment where no
    // window is set anywhere must not pay for one.
    check!(provider.expires_any_blocks());
    check!(
        !OverridesProvider::from_yaml("overrides:\n  tenant-a:\n    retention_period: \"0s\"\n")
            .expect("the overrides file parses")
            .expires_any_blocks()
    );
}
