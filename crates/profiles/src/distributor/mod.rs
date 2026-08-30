//! Distributor role: decode ingress doors, split profiles, and append WAL records.

use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use axum::{
    Extension, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, RawQuery},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use connectrpc_axum::{
    MakeServiceBuilder, MessageLimits,
    message::{Code, ConnectError, ConnectRequest, ConnectResponse},
};
use krabka_client_producer::{Header, Producer, ProducerRecord};
use krabka_pprof::PprofProfile;
use krabka_throttle::TokenBucket;
#[cfg(test)]
use krabka_units::mebibytes;
use krabka_units::{
    ByteSize, Frequency,
    convert::{ByteSizeExt, FrequencyExt as _, StdDurationExt as _},
};
use num_traits::ToPrimitive as _;
use prost::Message;
use tokio::net::TcpListener;
use tracing::Instrument as _;

use crate::{
    error::ProfilesError,
    ids::{IngestBytes, IngestItems},
    ingest::{
        LegacyDecodeLimits, RelabelConfig, TenantLimitConfig, apply_relabel, cap_session_id,
        decode_ingest_body_with_limits, decode_otlp, decode_push, enforce_limits,
        parse_ingest_query, require_service_name, split_sample_types,
    },
    limits::{Limits, OverridesProvider},
    metrics::ServiceMetrics,
    wal::{
        PROFILES_WAL_TOPIC, ProfileRecord, WalFunction, WalLocation, WalMapping, WalSample,
        WalSymbolSet, partition_key,
    },
    wire::pb,
};

#[cfg(test)]
mod tests {

    /// `merge_ingest_limits` takes each field from the override when that
    /// override is positive, and from the base otherwise. The four fields fall
    /// back independently, so every value here differs from every other: a
    /// field that reads its neighbour's override still produces a positive
    /// number, and only distinct values make that visible.
    #[test]
    fn ingest_limits_fall_back_field_by_field() {
        use krabka_units::bytes;

        let base = crate::ingest::TenantLimits {
            max_label_name: bytes(11),
            max_label_names_per_series: 22,
            max_label_value: bytes(33),
            session_id_buckets: 44,
        };
        let zeroed = super::Limits {
            max_label_name: bytes(0),
            max_label_value: bytes(0),
            max_label_names_per_series: 0,
            max_session_id_cardinality: 0,
            ..super::Limits::default()
        };

        // Every override unset: the base survives intact, field for field.
        let merged = super::merge_ingest_limits(&base, &zeroed);
        check!(merged.max_label_name == bytes(11));
        check!(merged.max_label_names_per_series == 22);
        check!(merged.max_label_value == bytes(33));
        check!(merged.session_id_buckets == 44);

        // Every override set: each replaces its own field and no other.
        let overridden = super::Limits {
            max_label_name: bytes(55),
            max_label_value: bytes(66),
            max_label_names_per_series: 77,
            max_session_id_cardinality: 88,
            ..super::Limits::default()
        };
        let merged = super::merge_ingest_limits(&base, &overridden);
        check!(merged.max_label_name == bytes(55));
        check!(merged.max_label_value == bytes(66));
        check!(merged.max_label_names_per_series == 77);
        check!(merged.session_id_buckets == 88);

        // One field overridden at a time, so a fallback that reads the wrong
        // side shows up as the other three changing when they should not.
        let one = super::Limits {
            max_label_value: bytes(66),
            ..zeroed.clone()
        };
        let merged = super::merge_ingest_limits(&base, &one);
        check!(merged.max_label_value == bytes(66), "the overridden one");
        check!(merged.max_label_name == bytes(11), "and only that one");
        check!(merged.max_label_names_per_series == 22);
        check!(merged.session_id_buckets == 44);
    }
    use std::sync::{Arc, Mutex};

    use assert2::{assert, check};
    use krabka_units::{bytes, per_sec};
    use prost::Message;

    use super::*;

    /// `positive_or` and `merge_ingest_limits` implement one rule: a
    /// per-tenant override counts only when it is set, and zero means unset.
    /// Every field has to make that choice independently, so the case below
    /// overrides exactly one field at a time and checks the other three still
    /// come from the base.
    #[test]
    fn a_zero_override_falls_back_to_the_base_limit_field_by_field() {
        let base = crate::ingest::TenantLimits {
            max_label_name: bytes(11),
            max_label_names_per_series: 22,
            max_label_value: bytes(33),
            session_id_buckets: 44,
        };

        // Limits::default() is not an empty override: its label caps are
        // real values that would legitimately win. "Unset" means zero, so
        // that is what the baseline here has to be.
        let unset = || Limits {
            max_label_name: bytes(0),
            max_label_value: bytes(0),
            max_label_names_per_series: 0,
            max_session_id_cardinality: 0,
            ..Limits::default()
        };

        check!(
            super::merge_ingest_limits(&base, &unset()) == base,
            "an override that sets nothing changes nothing"
        );

        // Each field on its own, with the rest left unset.
        check!(
            super::merge_ingest_limits(
                &base,
                &Limits {
                    max_label_name: bytes(1),
                    ..unset()
                }
            ) == crate::ingest::TenantLimits {
                max_label_name: bytes(1),
                ..base.clone()
            }
        );
        check!(
            super::merge_ingest_limits(
                &base,
                &Limits {
                    max_label_names_per_series: 2,
                    ..unset()
                }
            ) == crate::ingest::TenantLimits {
                max_label_names_per_series: 2,
                ..base.clone()
            }
        );
        check!(
            super::merge_ingest_limits(
                &base,
                &Limits {
                    max_label_value: bytes(3),
                    ..unset()
                }
            ) == crate::ingest::TenantLimits {
                max_label_value: bytes(3),
                ..base.clone()
            }
        );
        check!(
            super::merge_ingest_limits(
                &base,
                &Limits {
                    max_session_id_cardinality: 4,
                    ..unset()
                }
            ) == crate::ingest::TenantLimits {
                session_id_buckets: 4,
                ..base.clone()
            }
        );
    }

    /// `rate_tokens_per_sec` rounds a fractional rate up, floors it at one
    /// token so a trickle still admits something, and caps it at the burst
    /// size when one is configured.
    #[test]
    fn the_token_rate_rounds_up_floors_at_one_and_respects_the_burst_cap() {
        let rate = |per_second: f64, burst| {
            super::rate_tokens_per_sec(&Limits {
                ingestion_rate: Frequency::from_per_sec(per_second),
                ingestion_burst_profiles: burst,
                ..Limits::default()
            })
        };

        check!(
            rate(10.0, 0) == 10,
            "a whole rate with no cap passes through"
        );
        check!(rate(10.2, 0) == 11, "a fraction rounds up, not down");
        check!(rate(0.1, 0) == 1, "a trickle still admits one");
        check!(rate(0.0, 0) == 1, "so does nothing at all");

        // The cap binds only when it is both set and lower than the rate.
        check!(rate(10.0, 4) == 4, "a lower burst caps the rate");
        check!(rate(10.0, 10) == 10, "an equal burst leaves it alone");
        check!(rate(10.0, 40) == 10, "a higher burst does not raise it");
    }

    /// `u32_from_i64` names the field it could not convert, because the
    /// caller has several and the message is all that distinguishes them.
    #[test]
    fn narrowing_to_u32_names_the_field_that_did_not_fit() {
        check!(super::u32_from_i64(0, "width").unwrap() == 0);
        check!(super::u32_from_i64(i64::from(u32::MAX), "width").unwrap() == u32::MAX);

        let err = super::u32_from_i64(-1, "width").unwrap_err().to_string();
        check!(err.contains("width does not fit u32"), "got: {err}");
        let err = super::u32_from_i64(i64::from(u32::MAX) + 1, "height")
            .unwrap_err()
            .to_string();
        check!(err.contains("height does not fit u32"), "got: {err}");
    }

    fn state_with_ingestion(rate: f64, burst: u64, max_tenants: usize) -> Arc<DistributorState> {
        Arc::new(DistributorState {
            sink: Arc::new(RecordingSink(Mutex::default())),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(&format!(
                "overrides:\n  tenant-a:\n    ingestion_rate_profiles_per_sec: {rate}\n    ingestion_burst_profiles: {burst}\n"
            ))
            .expect("the overrides parse"),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: max_tenants,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        })
    }

    /// A burst cap rejects an over-sized batch outright, before the token
    /// bucket is consulted.
    ///
    /// The `> 0` in that guard cannot be tested from outside:
    /// `rate_tokens_per_sec` clamps the bucket's own rate to the burst
    /// whenever the burst is positive, so any batch the guard would reject
    /// the bucket rejects too, with the same error. The guard is an early
    /// exit, not a distinct behaviour. What IS pinned here is the zero case
    /// -- a zero burst means unlimited rather than "reject everything" --
    /// and the boundary, where a batch of exactly the burst is allowed.
    #[test]
    fn a_burst_cap_rejects_an_over_sized_batch_before_the_bucket() {
        let state = state_with_ingestion(1_000_000.0, 2, 4096);

        check!(
            super::enforce_ingestion_rate(&state, "tenant-a", 2).is_ok(),
            "at the cap"
        );
        check!(
            super::enforce_ingestion_rate(&state, "tenant-a", 3).is_err(),
            "one over the cap, with a rate that would otherwise allow it"
        );

        // A zero burst means "no burst cap", not "reject everything", so the
        // guard must be `> 0` rather than a plain non-zero test.
        let unlimited = state_with_ingestion(1_000_000.0, 0, 4096);
        check!(super::enforce_ingestion_rate(&unlimited, "tenant-a", 5_000).is_ok());

        // A tenant with no override of its own is not rate limited at all --
        // asked for well past the DEFAULT burst of 10_000, which is what
        // separates "skipped entirely" from "happened to fit under the
        // default cap".
        check!(super::enforce_ingestion_rate(&state, "tenant-b", 20_000).is_ok());
        // And an empty batch is never rejected, whatever the caps say.
        check!(super::enforce_ingestion_rate(&state, "tenant-a", 0).is_ok());
    }

    /// The per-tenant bucket map is capped, evicting one existing tenant
    /// before admitting a new one. The cap is only reached by admitting more
    /// tenants than it allows, and the eviction is only observable in the
    /// map's size -- so the test counts buckets rather than trusting a
    /// tenant to still be present.
    #[test]
    fn the_bucket_map_evicts_before_admitting_a_tenant_past_its_cap() {
        let state = state_with_ingestion(1_000_000.0, 0, 2);
        let rate = krabka_units::Frequency::from_per_sec_u64(10);
        let buckets = |state: &DistributorState| {
            state
                .ingestion_buckets
                .lock()
                .expect("the bucket lock is held")
                .len()
        };

        for tenant in ["a", "b"] {
            super::ingestion_bucket_for_tenant(&state, tenant, rate).expect("a bucket is issued");
        }
        check!(buckets(&state) == 2, "both tenants fit under the cap");

        // The third tenant is one past the cap, so admitting it must evict.
        super::ingestion_bucket_for_tenant(&state, "c", rate).expect("a bucket is issued");
        check!(buckets(&state) == 2, "the map does not grow past its cap");

        // Re-asking for a tenant already held must not evict anything. Which
        // tenant an eviction picks is arbitrary, so both consequences are
        // checked: dropping some other tenant shrinks the map, and dropping
        // this one hands back a different bucket.
        let before =
            super::ingestion_bucket_for_tenant(&state, "c", rate).expect("a bucket is issued");
        let again =
            super::ingestion_bucket_for_tenant(&state, "c", rate).expect("a bucket is issued");
        check!(buckets(&state) == 2, "no other tenant was evicted");
        check!(
            Arc::ptr_eq(&before, &again),
            "and this tenant kept its bucket"
        );
    }

    fn state_with_max_series(limit: u64) -> Arc<DistributorState> {
        Arc::new(DistributorState {
            sink: Arc::new(RecordingSink(Mutex::default())),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(&format!(
                "overrides:\n  tenant-a:\n    max_series: {limit}\n"
            ))
            .unwrap(),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        })
    }

    fn series_record(name: &str) -> ProfileRecord {
        ProfileRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), name.to_string())],
            profile_type: "cpu".to_string(),
            samples: vec![],
            symbols: crate::wal::WalSymbolSet {
                strings: vec![],
                functions: vec![],
                locations: vec![],
                mappings: vec![],
            },
        }
    }

    /// The max-series budget counts *distinct new* fingerprints, reserves them
    /// only if they all fit, and leaves the tenant's set untouched when they
    /// do not. That last property is the one worth guarding: a rejection that
    /// half-reserved would burn budget on a request that never landed.
    #[test]
    fn max_series_reserves_all_or_nothing() {
        let state = state_with_max_series(2);
        let reserved = |records: &[ProfileRecord]| {
            super::enforce_and_reserve_max_series(&state, "tenant-a", records)
        };
        let held = || {
            state
                .active_series
                .lock()
                .unwrap()
                .get("tenant-a")
                .map_or(0, std::collections::BTreeSet::len)
        };

        // A repeated series counts once, so this fits a budget of two.
        let two = [series_record("a"), series_record("b"), series_record("a")];
        check!(
            reserved(&two).unwrap().len() == 2,
            "three records, two series"
        );
        check!(held() == 2);

        // Re-offering what is already held adds nothing and stays within budget.
        check!(reserved(&two).unwrap().is_empty(), "nothing new to reserve");
        check!(held() == 2);

        // One more distinct series is over the limit, and is refused whole.
        let err = reserved(&[series_record("c")]).unwrap_err().to_string();
        check!(err.contains("max series exceeded"), "got: {err}");
        check!(held() == 2, "a rejected request reserves nothing");

        // Even a request that mixes a known series with a new one is refused
        // in full rather than admitting the part that fits.
        let err = reserved(&[series_record("a"), series_record("d")])
            .unwrap_err()
            .to_string();
        check!(err.contains("max series exceeded"), "got: {err}");
        check!(held() == 2, "still nothing reserved");
    }

    /// A limit of zero means unlimited, so nothing is tracked at all.
    #[test]
    fn a_max_series_limit_of_zero_tracks_nothing() {
        let state = state_with_max_series(0);
        let records = [series_record("a"), series_record("b")];

        check!(
            super::enforce_and_reserve_max_series(&state, "tenant-a", &records)
                .unwrap()
                .is_empty()
        );
        check!(
            state.active_series.lock().unwrap().is_empty(),
            "no tenant is tracked"
        );
    }

    /// pprof ids are indexes into a reference table. The optional form treats
    /// zero as "absent" and returns zero without a lookup; the required form
    /// has no such case and must reject zero like any other unknown id.
    #[test]
    fn pprof_ids_resolve_through_the_reference_table() {
        let refs = HashMap::from([(7_u64, 1_u32), (9, 2)]);

        check!(
            super::normalize_optional_pprof_id(0, &refs, "f").unwrap() == 0,
            "zero is absent"
        );
        check!(super::normalize_optional_pprof_id(7, &refs, "f").unwrap() == 1);
        check!(super::normalize_optional_pprof_id(9, &refs, "f").unwrap() == 2);
        let err = super::normalize_optional_pprof_id(8, &refs, "location")
            .unwrap_err()
            .to_string();
        check!(
            err.contains("location references missing id 8"),
            "got: {err}"
        );

        // The required form differs only in how it treats zero.
        check!(super::normalize_required_pprof_id(7, &refs, "f").unwrap() == 1);
        let err = super::normalize_required_pprof_id(0, &refs, "function")
            .unwrap_err()
            .to_string();
        check!(
            err.contains("function references missing id 0"),
            "got: {err}"
        );
    }

    /// Every limit breach maps to the status the client should act on:
    /// back off, or stop sending this shape of request.
    #[test]
    fn every_limit_error_maps_to_the_code_the_client_should_act_on() {
        let cases = [
            (
                crate::limits::LimitError::IngestionRateExceeded {
                    rate: 1.0,
                    observed: 2.0,
                },
                Code::ResourceExhausted,
            ),
            (
                crate::limits::LimitError::MaxSeries {
                    limit: 1,
                    observed: 2,
                },
                Code::ResourceExhausted,
            ),
            (
                crate::limits::LimitError::SessionCardinalityExceeded { limit: 1 },
                Code::ResourceExhausted,
            ),
            (
                crate::limits::LimitError::LabelNameTooLong {
                    limit: 1,
                    observed: 2,
                },
                Code::InvalidArgument,
            ),
            (
                crate::limits::LimitError::LabelValueTooLong {
                    limit: 1,
                    observed: 2,
                },
                Code::InvalidArgument,
            ),
            (
                crate::limits::LimitError::TooManyLabels {
                    limit: 1,
                    observed: 2,
                },
                Code::InvalidArgument,
            ),
            (
                crate::limits::LimitError::QueryLengthExceeded {
                    limit_secs: 1,
                    observed_secs: 2,
                },
                Code::InvalidArgument,
            ),
        ];

        for (err, expected) in cases {
            check!(super::limit_connect_code(&err) == expected, "for {err}");
        }
    }

    /// `ingest_span_tenant` reads the `X-Scope-OrgID` header. It returns the
    /// value verbatim when the header is present and non-empty. It returns
    /// `"unknown"` when the header is missing or empty.
    #[test]
    fn ingest_span_tenant_reads_scope_orgid_header() {
        let mut present = HeaderMap::new();
        present.insert("x-scope-orgid", "acme".parse().unwrap());
        assert!(ingest_span_tenant(&present) == "acme");

        let missing = HeaderMap::new();
        assert!(ingest_span_tenant(&missing) == "unknown");

        let mut empty = HeaderMap::new();
        empty.insert("x-scope-orgid", "".parse().unwrap());
        assert!(ingest_span_tenant(&empty) == "unknown");
    }

    use crate::{
        error::ProfilesError,
        ingest::{RelabelAction, RelabelConfig, TenantLimitConfig, TenantLimits},
        limits::OverridesProvider,
        wal::ProfileRecord,
    };

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<ProfileRecord>>);

    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, rec: ProfileRecord) -> Result<(), ProfilesError> {
            self.0.lock().unwrap().push(rec);
            Ok(())
        }
    }

    /// A sink whose `append` always fails, to exercise the WAL-failure
    /// reservation-rollback path.
    struct FailingSink;

    #[async_trait::async_trait]
    impl WalSink for FailingSink {
        async fn append(&self, _rec: ProfileRecord) -> Result<(), ProfilesError> {
            Err(ProfilesError::Produce(
                "simulated produce failure".to_string(),
            ))
        }
    }

    fn state_with(sink: Arc<RecordingSink>) -> Arc<DistributorState> {
        Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::new(Limits::default()),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        })
    }

    fn otlp_export_request() -> pb::otlp_profiles::ExportProfilesServiceRequest {
        use pb::{
            opentelemetry::proto::{
                common::v1::{AnyValue, KeyValue, any_value::Value},
                resource::v1::Resource,
            },
            otlp_profiles::{
                Function, Line, Location, Profile, ProfilesDictionary, ResourceProfiles, Sample,
                ScopeProfiles, Stack, ValueType,
            },
        };

        let dictionary = ProfilesDictionary {
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "main".to_string(),
            ],
            function_table: vec![Function {
                name_strindex: 3,
                ..Default::default()
            }],
            location_table: vec![Location {
                address: 0x40,
                lines: vec![Line {
                    function_index: 0,
                    line: 1,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            stack_table: vec![Stack {
                location_indices: vec![0],
            }],
            ..Default::default()
        };
        let profile = Profile {
            sample_type: Some(ValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            period_type: Some(ValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            samples: vec![Sample {
                stack_index: 0,
                values: vec![7],
                timestamps_unix_nano: vec![1_700_000_000_000_000_000],
                ..Default::default()
            }],
            time_unix_nano: 1_700_000_000_000_000_000,
            ..Default::default()
        };

        pb::otlp_profiles::ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("api".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_profiles: vec![ScopeProfiles {
                    profiles: vec![profile],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            dictionary: Some(dictionary),
        }
    }

    #[tokio::test]
    async fn push_splits_and_appends_one_record_per_sample_type() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let raws = vec![crate::wire::test_fixtures::raw_profile_2types()];

        process_raw(&state, "tenant-a", raws).await.unwrap();

        let recs = sink.0.lock().unwrap();
        check!(recs.len() == 2);
        check!(recs.iter().all(|rec| rec.tenant == "tenant-a"));
        check!(
            recs.iter()
                .all(|rec| rec.labels.iter().any(|(name, _)| name == "service_name"))
        );
    }

    #[tokio::test]
    async fn push_normalizes_pprof_symbol_ids_to_wal_indices() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");
        let profile = PprofProfile::from(krabka_pprof::proto::Profile {
            sample_type: vec![krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![krabka_pprof::proto::Sample {
                location_id: vec![2],
                value: vec![5],
                label: Vec::new(),
            }],
            location: vec![
                krabka_pprof::proto::Location {
                    id: 1,
                    line: vec![krabka_pprof::proto::Line {
                        function_id: 1,
                        line: 10,
                        column: 0,
                    }],
                    ..Default::default()
                },
                krabka_pprof::proto::Location {
                    id: 2,
                    line: vec![krabka_pprof::proto::Line {
                        function_id: 2,
                        line: 20,
                        column: 0,
                    }],
                    ..Default::default()
                },
            ],
            function: vec![
                krabka_pprof::proto::Function {
                    id: 1,
                    name: 3,
                    system_name: 3,
                    filename: 5,
                    start_line: 1,
                },
                krabka_pprof::proto::Function {
                    id: 2,
                    name: 4,
                    system_name: 4,
                    filename: 5,
                    start_line: 2,
                },
            ],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "first".to_string(),
                "second".to_string(),
                "main.go".to_string(),
            ],
            period_type: Some(krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }),
            ..Default::default()
        });

        process_raw(
            &state,
            "tenant-a",
            vec![crate::ingest::RawProfile {
                labels,
                profile,
                delta: false,
                sample_timestamps_ns: Vec::new(),
                sample_span_ids: Vec::new(),
                sample_trace_ids: Vec::new(),
            }],
        )
        .await
        .unwrap();

        let recs = sink.0.lock().unwrap();
        assert!(recs[0].samples[0].stacktrace_location_refs == vec![1]);
        assert!(recs[0].symbols.locations[1].lines[0].0 == 1);
    }

    #[tokio::test]
    async fn relabel_drop_skips_the_series() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::new(Limits::default()),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![RelabelConfig {
                source_labels: vec!["__name__".to_string()],
                regex: "process_cpu".to_string(),
                target_label: String::new(),
                replacement: String::new(),
                action: RelabelAction::Drop,
            }],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });
        let raws = vec![crate::wire::test_fixtures::raw_profile_cpu()];

        process_raw(&state, "t", raws).await.unwrap();

        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn tenant_specific_limits_are_enforced() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default().with_tenant_limits(
                "tenant-a",
                TenantLimits {
                    max_label_value: bytes(3),
                    ..Default::default()
                },
            ),
            profile_overrides: OverridesProvider::new(Limits::default()),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();
        process_raw(
            &state,
            "tenant-b",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();

        assert!(err.to_string().contains("value exceeds"));
    }

    #[tokio::test]
    async fn label_count_limit_is_enforced_after_profile_type_split() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default().with_tenant_limits(
                "tenant-a",
                TenantLimits {
                    max_label_names_per_series: 5,
                    ..Default::default()
                },
            ),
            profile_overrides: OverridesProvider::new(Limits::default()),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("too many label names"));
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pyroscope_overrides_drive_ingest_label_limits() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r"
overrides:
  tenant-a:
    max_label_value_length: 3
",
            )
            .unwrap(),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();
        process_raw(
            &state,
            "tenant-b",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();

        assert!(err.to_string().contains("value exceeds"));
    }

    #[tokio::test]
    async fn pyroscope_overrides_enforce_max_series_without_partial_writes() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r"
overrides:
  tenant-a:
    max_series: 1
",
            )
            .unwrap(),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("max series exceeded"), "{err}");
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pyroscope_overrides_enforce_ingestion_burst_without_partial_writes() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 100
    ingestion_burst_profiles: 1
",
            )
            .unwrap(),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("ingestion rate exceeded"), "{err}");
        assert!(sink.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pyroscope_overrides_enforce_ingestion_rate_per_tenant() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 1
    ingestion_burst_profiles: 1
",
            )
            .unwrap(),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();
        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap_err();
        process_raw(
            &state,
            "tenant-b",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();

        assert!(err.to_string().contains("ingestion rate exceeded"), "{err}");
        assert!(sink.0.lock().unwrap().len() == 2);
    }

    #[test]
    fn limit_errors_map_to_resource_exhausted_connect_code() {
        let err = connect_error(
            crate::limits::LimitError::MaxSeries {
                limit: 1,
                observed: 2,
            }
            .into(),
        );

        assert!(err.code() == Code::ResourceExhausted);
        assert!(
            err.message()
                .is_some_and(|message| message.contains("max series exceeded"))
        );
    }

    fn push_request_one_sample() -> pb::push::v1::PushRequest {
        use std::io::Write as _;

        let pprof_bytes = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&pprof_bytes).unwrap();
        let gzipped = encoder.finish().unwrap();

        pb::push::v1::PushRequest {
            series: vec![pb::push::v1::RawProfileSeries {
                labels: vec![
                    pb::types::v1::LabelPair {
                        name: "__name__".into(),
                        value: "process_cpu".into(),
                    },
                    pb::types::v1::LabelPair {
                        name: "service_name".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![pb::push::v1::RawSample {
                    raw_profile: gzipped,
                    id: "s1".into(),
                }],
                annotations: Vec::new(),
            }],
        }
    }

    // The Connect `push` handler must decode the request, append the decoded
    // profile to the WAL sink, and record the ingest metrics. A body replaced
    // with a bare `Ok(Default::default())` would append nothing.
    #[tokio::test]
    async fn push_handler_appends_record_and_records_metrics() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let mut headers = HeaderMap::new();
        headers.insert("x-scope-orgid", "tenant-a".parse().unwrap());

        push_handler(
            Extension(state.clone()),
            headers,
            ConnectRequest(push_request_one_sample()),
        )
        .await
        .unwrap();

        let recs = sink.0.lock().unwrap();
        check!(recs.len() == 1);
        check!(recs[0].tenant == "tenant-a");
        // Metrics side effect: one ok ingest request was recorded.
        check!(
            state
                .metrics
                .ingest_requests
                .get_or_create(&crate::metrics::StatusLabel {
                    status: "ok".into(),
                })
                .get()
                == 1
        );
    }

    // The Connect `export` (OTLP) handler must decode the request, append the
    // decoded profile to the WAL sink, and record the ingest metrics. A body
    // replaced with a bare `Ok(Default::default())` would append nothing.
    #[tokio::test]
    async fn export_handler_appends_record_and_records_metrics() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let mut headers = HeaderMap::new();
        headers.insert("x-scope-orgid", "tenant-a".parse().unwrap());

        export_handler(
            Extension(state.clone()),
            headers,
            ConnectRequest(otlp_export_request()),
        )
        .await
        .unwrap();

        let recs = sink.0.lock().unwrap();
        check!(recs.len() == 1);
        check!(recs[0].tenant == "tenant-a");
        check!(
            state
                .metrics
                .ingest_requests
                .get_or_create(&crate::metrics::StatusLabel {
                    status: "ok".into(),
                })
                .get()
                == 1
        );
    }

    #[tokio::test]
    async fn otlp_http_profiles_path_appends_records() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();
        let body = otlp_export_request().encode_to_vec();

        let response = reqwest::Client::new()
            .post(format!("http://{bound}/v1development/profiles"))
            .header("content-type", "application/x-protobuf")
            .header("x-scope-orgid", "tenant-a")
            .body(body)
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK, "{response:?}");
        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 1);
        check!(recs[0].tenant == "tenant-a");
        check!(recs[0].labels.iter().any(|(name, value)| {
            name == "__profile_type__" && value == "samples:samples:count:samples:count"
        }));
    }

    #[tokio::test]
    async fn legacy_ingest_accepts_plain_folded_groups_body() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!(
                "http://{bound}/ingest?name=myapp{{service_name=\"api\"}}&format=groups&units=samples&until=1700000000000"
            ))
            .header("content-type", "text/plain")
            .header("x-scope-orgid", "tenant-a")
            .body("main;work 3\n")
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK, "{response:?}");
        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 1);
        check!(recs[0].tenant == "tenant-a");
        for (name, value) in [
            ("__profile_type__", "myapp:samples:samples:samples:samples"),
            ("service_name", "api"),
        ] {
            check!(
                recs[0]
                    .labels
                    .iter()
                    .any(|(label_name, label_value)| label_name == name && label_value == value)
            );
        }
        assert!(recs[0].samples.len() == 1);
        check!(recs[0].samples[0].value == 3);
        check!(recs[0].samples[0].timestamp_ns == 1_700_000_000_000_000_000);
    }

    #[tokio::test]
    async fn legacy_ingest_limit_errors_return_connect_shaped_json() {
        let response = profiles_error_response(
            crate::limits::LimitError::MaxSeries {
                limit: 1,
                observed: 2,
            }
            .into(),
        );
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        check!(status == StatusCode::TOO_MANY_REQUESTS);
        check!(json.get("code").and_then(serde_json::Value::as_str) == Some("resource_exhausted"));
        check!(
            json.get("message")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|message| message.contains("max series exceeded"))
        );
    }

    // C1: the ingest door must validate the `X-Scope-OrgID` tenant.
    #[tokio::test]
    async fn ingest_rejects_path_unsafe_tenant_with_400() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!(
                "http://{bound}/ingest?name=myapp{{service_name=\"api\"}}&format=groups&units=samples&until=1700000000000"
            ))
            .header("content-type", "text/plain")
            .header("x-scope-orgid", "../escape")
            .body("main;work 3\n")
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::BAD_REQUEST, "{response:?}");
        // A rejected tenant must not produce any WAL records.
        assert!(sink.0.lock().unwrap().is_empty());
    }

    // C1: an absent header still defaults to the anonymous tenant.
    #[tokio::test]
    async fn ingest_without_tenant_header_defaults_to_anonymous() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let (_shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let bound = serve("127.0.0.1:0".parse().unwrap(), state, async move {
            let _ = shutdown_rx.await;
        })
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!(
                "http://{bound}/ingest?name=myapp{{service_name=\"api\"}}&format=groups&units=samples&until=1700000000000"
            ))
            .header("content-type", "text/plain")
            .body("main;work 3\n")
            .send()
            .await
            .unwrap();

        assert!(response.status() == StatusCode::OK, "{response:?}");
        let recs = sink.0.lock().unwrap();
        assert!(recs.len() == 1);
        assert!(recs[0].tenant == "anonymous");
    }

    #[test]
    fn tenant_from_headers_validates_and_defaults() {
        use axum::http::HeaderValue;

        let mut headers = HeaderMap::new();
        assert!(tenant_from_headers(&headers).unwrap() == "anonymous");

        headers.insert("x-scope-orgid", HeaderValue::from_static("tenant-a"));
        assert!(tenant_from_headers(&headers).unwrap() == "tenant-a");

        headers.insert("x-scope-orgid", HeaderValue::from_static("a/b"));
        assert!(tenant_from_headers(&headers).is_err());
    }

    // #3: when the WAL append fails, the max-series reservation is rolled back
    // so a failed write does not permanently consume the tenant's budget.
    #[tokio::test]
    async fn wal_append_failure_rolls_back_max_series_reservation() {
        let state = Arc::new(DistributorState {
            sink: Arc::new(FailingSink),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r"
overrides:
  tenant-a:
    max_series: 100
",
            )
            .unwrap(),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, ProfilesError::Produce(_)), "{err}");

        // The reservation must have been rolled back: no leftover fingerprints.
        let active = state.active_series.lock().unwrap();
        assert!(
            active
                .get("tenant-a")
                .map_or(0, std::collections::BTreeSet::len)
                == 0
        );
    }

    // #3: a max-series rejection leaves the tracked set untouched (no partial
    // reservation), so a subsequent within-budget write still succeeds.
    #[tokio::test]
    async fn max_series_rejection_does_not_reserve() {
        let sink = Arc::new(RecordingSink::default());
        let state = Arc::new(DistributorState {
            sink: sink.clone(),
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::from_yaml(
                r"
overrides:
  tenant-a:
    max_series: 1
",
            )
            .unwrap(),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        // Two distinct series in one request exceed the cap of 1 and are rejected.
        let err = process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_2types()],
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("max series exceeded"), "{err}");

        // Nothing was reserved, so a single-series write afterwards succeeds.
        process_raw(
            &state,
            "tenant-a",
            vec![crate::wire::test_fixtures::raw_profile_cpu()],
        )
        .await
        .unwrap();
        assert!(sink.0.lock().unwrap().len() == 1);
    }

    // #4: the per-tenant maps are bounded; admitting tenant N+1 past the cap
    // evicts an existing tenant rather than growing without limit.
    #[test]
    fn evict_one_tenant_bounds_map_growth() {
        let mut map: HashMap<String, usize> = HashMap::new();
        for idx in 0..4096 {
            map.insert(format!("tenant-{idx}"), idx);
        }
        assert!(map.len() == 4096);

        // Simulate the admission guard: evict before inserting a new tenant.
        if !map.contains_key("tenant-new") && map.len() >= 4096 {
            evict_one_tenant(&mut map);
        }
        map.insert("tenant-new".to_string(), 0);

        assert!(map.len() == 4096);
        assert!(map.contains_key("tenant-new"));
    }

    #[tokio::test]
    async fn ingestion_buckets_map_is_capped() {
        let sink = Arc::new(RecordingSink::default());
        // Build an overrides provider that gives EVERY tenant a finite rate, so
        // each distinct tenant allocates a bucket. We assert the map never grows
        // past `MAX_TENANTS`.
        let state = Arc::new(DistributorState {
            sink,
            limits: TenantLimitConfig::default(),
            profile_overrides: OverridesProvider::new(crate::limits::Limits {
                ingestion_rate: per_sec(1000),
                ingestion_burst_profiles: 1000,
                ..Default::default()
            }),
            active_series: Mutex::default(),
            ingestion_buckets: Mutex::default(),
            relabel: vec![],
            max_decompressed: mebibytes(16),
            max_tracked_tenants: 4096,
            legacy_decode_limits: LegacyDecodeLimits::default(),
            metrics: ServiceMetrics::new(),
        });

        for idx in 0..(4096 + 50) {
            // `has_tenant_override` is false for the default provider, so the
            // rate path is skipped; allocate buckets directly to exercise the cap.
            let _ = ingestion_bucket_for_tenant(&state, &format!("tenant-{idx}"), per_sec(10));
        }

        let buckets = state.ingestion_buckets.lock().unwrap();
        assert!(
            buckets.len() <= state.max_tracked_tenants,
            "{}",
            buckets.len()
        );
    }

    // #7: a 5xx/internal error returns a GENERIC body, not the detailed text.
    #[tokio::test]
    async fn internal_error_response_is_generic() {
        let response = profiles_error_response(ProfilesError::Produce(
            "kafka broker is on fire".to_string(),
        ));
        let status = response.status();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8_lossy(&body);

        check!(status == StatusCode::INTERNAL_SERVER_ERROR);
        check!(text == INTERNAL_ERROR_MESSAGE, "leaked detail: {text}");
        check!(!text.contains("kafka"), "leaked detail: {text}");
    }

    // #7: a 4xx client-input error keeps its specific, useful message.
    #[test]
    fn client_input_error_keeps_specific_message() {
        let message = client_facing_message(&ProfilesError::Invalid("bad query param".to_string()));
        assert!(message.contains("bad query param"), "{message}");
    }

    // #7: a poisoned lock is now an Internal/500 with a generic message, not a 400.
    #[test]
    fn poisoned_lock_maps_to_internal_500() {
        let err = ProfilesError::Internal("active series lock poisoned".to_string());
        assert!(err.status_code() == 500);

        let connect = connect_error(ProfilesError::Internal("secret detail".to_string()));
        assert!(connect.code() == Code::Internal);
        assert!(
            connect
                .message()
                .is_some_and(|message| message == INTERNAL_ERROR_MESSAGE)
        );
    }

    // #11: mapping symbolization flags flow through independently.
    #[tokio::test]
    async fn mapping_symbolization_flags_are_populated_independently() {
        let sink = Arc::new(RecordingSink::default());
        let state = state_with(sink.clone());
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");
        let profile = PprofProfile::from(krabka_pprof::proto::Profile {
            sample_type: vec![krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![krabka_pprof::proto::Sample {
                location_id: vec![1],
                value: vec![5],
                label: Vec::new(),
            }],
            location: vec![krabka_pprof::proto::Location {
                id: 1,
                mapping_id: 1,
                line: vec![krabka_pprof::proto::Line {
                    function_id: 1,
                    line: 10,
                    column: 0,
                }],
                ..Default::default()
            }],
            function: vec![krabka_pprof::proto::Function {
                id: 1,
                name: 3,
                system_name: 3,
                filename: 4,
                start_line: 1,
            }],
            mapping: vec![krabka_pprof::proto::Mapping {
                id: 1,
                memory_start: 0x1000,
                memory_limit: 0x2000,
                file_offset: 0,
                filename: 5,
                build_id: 0,
                // Deliberately mixed: functions+line numbers symbolized, but no
                // filenames and no inline frames. A correct mapping must NOT
                // collapse these onto `has_functions`.
                symbolization: krabka_pprof::proto::MappingSymbolization::from_parts((
                    true, false, true, false,
                )),
            }],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "main".to_string(),
                "main.go".to_string(),
                "bin".to_string(),
            ],
            period_type: Some(krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }),
            ..Default::default()
        });

        process_raw(
            &state,
            "tenant-a",
            vec![crate::ingest::RawProfile {
                labels,
                profile,
                delta: false,
                sample_timestamps_ns: Vec::new(),
                sample_span_ids: Vec::new(),
                sample_trace_ids: Vec::new(),
            }],
        )
        .await
        .unwrap();

        let recs = sink.0.lock().unwrap();
        let mapping = &recs[0].symbols.mappings[0];
        check!(mapping.has_functions.get());
        check!(!mapping.has_filenames.get());
        check!(mapping.has_line_numbers.get());
        check!(!mapping.has_inline_frames.get());
    }
}

mod client_facing_message;
mod connect_error;
mod distributor_state;
mod enforce_and_reserve_max_series;
mod enforce_ingestion_rate;
mod evict_one_tenant;
mod export_handler;
mod extract_symbols;
mod ingest_handler;
mod ingest_limits_for_tenant;
mod ingest_span_tenant;
mod ingestion_bucket_for_tenant;
mod internal_error_message;
mod kafka_sink;
mod limit_connect_code;
mod merge_ingest_limits;
mod normalize_optional_pprof_id;
mod normalize_required_pprof_id;
mod otlp_http_handler;
mod positive_or;
mod process_raw;
mod profiles_error_response;
mod push_handler;
mod rate_tokens_per_sec;
mod rollback_reserved_series;
mod router;
mod serve;
mod serve_supervised;
mod tenant_from_headers;
mod u32_from_i64;
mod wal_sink;

use client_facing_message::client_facing_message;
use connect_error::connect_error;
pub use distributor_state::DistributorState;
use enforce_and_reserve_max_series::enforce_and_reserve_max_series;
use enforce_ingestion_rate::enforce_ingestion_rate;
use evict_one_tenant::evict_one_tenant;
use export_handler::export_handler;
use extract_symbols::extract_symbols;
use ingest_handler::ingest_handler;
use ingest_limits_for_tenant::ingest_limits_for_tenant;
use ingest_span_tenant::ingest_span_tenant;
use ingestion_bucket_for_tenant::ingestion_bucket_for_tenant;
use internal_error_message::INTERNAL_ERROR_MESSAGE;
pub use kafka_sink::KafkaSink;
use limit_connect_code::limit_connect_code;
use merge_ingest_limits::merge_ingest_limits;
use normalize_optional_pprof_id::normalize_optional_pprof_id;
use normalize_required_pprof_id::normalize_required_pprof_id;
use otlp_http_handler::otlp_http_handler;
use positive_or::positive_or;
pub use process_raw::process_raw;
use profiles_error_response::profiles_error_response;
use push_handler::push_handler;
use rate_tokens_per_sec::rate_tokens_per_sec;
use rollback_reserved_series::rollback_reserved_series;
pub use router::router;
pub use serve::serve;
pub use serve_supervised::serve_supervised;
use tenant_from_headers::tenant_from_headers;
use u32_from_i64::u32_from_i64;
pub use wal_sink::WalSink;
