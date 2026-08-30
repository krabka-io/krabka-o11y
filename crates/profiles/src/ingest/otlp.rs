//! OTLP `v1development` profiles -> `Vec<RawProfile>`.
//!
//! The generated OTLP types live in this crate, so the edge converts them into
//! the pprof wire model owned by `krabka-pprof`.

use krabka_blockstore::Labels;
use krabka_pprof::PprofProfile;

use crate::{error::ProfilesError, ingest::RawProfile, wire::pb};

#[cfg(test)]
mod tests {

    /// `resolve_service_name` reads `service.name` from the resource, and
    /// falls back to a fixed placeholder for every way that can fail. Each way
    /// is checked separately, since they reach the fallback by different
    /// routes and a guard removed from one is invisible to the others.
    #[test]
    fn a_missing_service_name_falls_back_rather_than_erroring() {
        use pb::opentelemetry::proto::{
            common::v1::{AnyValue, KeyValue, any_value::Value},
            resource::v1::Resource,
        };

        let with_attrs = |attrs: Vec<KeyValue>| pb::otlp_profiles::ResourceProfiles {
            resource: Some(Resource {
                attributes: attrs,
                ..Resource::default()
            }),
            ..pb::otlp_profiles::ResourceProfiles::default()
        };
        let attr = |key: &str, value: Option<Value>| KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value }),
        };

        // The name is found and returned as written.
        check!(
            super::resolve_service_name(&with_attrs(vec![attr(
                "service.name",
                Some(Value::StringValue("checkout".into()))
            )])) == "checkout"
        );

        // Found among others rather than only as the first attribute.
        check!(
            super::resolve_service_name(&with_attrs(vec![
                attr("host.name", Some(Value::StringValue("box".into()))),
                attr("service.name", Some(Value::StringValue("checkout".into()))),
            ])) == "checkout"
        );

        // Every route to the fallback.
        check!(
            super::resolve_service_name(&pb::otlp_profiles::ResourceProfiles::default())
                == "unknown_service",
            "no resource at all"
        );
        check!(
            super::resolve_service_name(&with_attrs(Vec::new())) == "unknown_service",
            "a resource with no attributes"
        );
        check!(
            super::resolve_service_name(&with_attrs(vec![attr(
                "host.name",
                Some(Value::StringValue("box".into()))
            )])) == "unknown_service",
            "the wrong key"
        );
        check!(
            super::resolve_service_name(&with_attrs(vec![attr("service.name", None)]))
                == "unknown_service",
            "the key with no value"
        );
        check!(
            super::resolve_service_name(&with_attrs(vec![attr(
                "service.name",
                Some(Value::IntValue(7))
            )])) == "unknown_service",
            "a value that is not a string"
        );
        check!(
            super::resolve_service_name(&with_attrs(vec![attr(
                "service.name",
                Some(Value::StringValue(String::new()))
            )])) == "unknown_service",
            "an empty name is not a name"
        );
    }

    /// `otlp_profile_to_pprof` renumbers OTLP's zero-based table indexes into
    /// pprof's one-based ids and copies each table across field by field.
    ///
    /// Every table here holds two entries with values that differ in every
    /// field, and the second entry is the one referenced, so an off-by-one in
    /// the renumbering and a pair of transposed fields both change the result.
    /// The whole decoded profile is compared at once.
    #[test]
    fn otlp_tables_are_renumbered_one_based_and_copied_field_by_field() {
        use pb::otlp_profiles::{
            Function, Line, Location, Mapping, Profile, ProfilesDictionary, Sample, Stack,
            ValueType,
        };

        let dict = ProfilesDictionary {
            //             0   1          2        3       4       5        6
            string_table: [
                "", "samples", "count", "fn_a", "fn_b", "sys_a", "sys_b",
                //             7        8        9        10
                "file_a", "file_b", "map_a", "map_b",
            ]
            .into_iter()
            .map(String::from)
            .collect(),
            mapping_table: vec![
                Mapping {
                    memory_start: 0x10,
                    memory_limit: 0x20,
                    file_offset: 0x30,
                    filename_strindex: 9,
                    ..Default::default()
                },
                Mapping {
                    memory_start: 0x40,
                    memory_limit: 0x50,
                    file_offset: 0x60,
                    filename_strindex: 10,
                    ..Default::default()
                },
            ],
            function_table: vec![
                Function {
                    name_strindex: 3,
                    system_name_strindex: 5,
                    filename_strindex: 7,
                    start_line: 11,
                },
                Function {
                    name_strindex: 4,
                    system_name_strindex: 6,
                    filename_strindex: 8,
                    start_line: 22,
                },
            ],
            location_table: vec![
                Location {
                    mapping_index: 0,
                    address: 0x100,
                    lines: vec![Line {
                        function_index: 0,
                        line: 1,
                        column: 2,
                    }],
                    ..Default::default()
                },
                // References the *second* mapping and function, so a
                // renumbering that is off by one lands somewhere visible.
                Location {
                    mapping_index: 1,
                    address: 0x200,
                    lines: vec![Line {
                        function_index: 1,
                        line: 3,
                        column: 4,
                    }],
                    ..Default::default()
                },
            ],
            stack_table: vec![Stack {
                location_indices: vec![1, 0],
            }],
            ..Default::default()
        };

        let profile = Profile {
            sample_type: Some(ValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            period_type: Some(ValueType {
                type_strindex: 2,
                unit_strindex: 1,
            }),
            period: 99,
            time_unix_nano: 1_700_000_000_000_000_000,
            duration_nano: 5_000,
            samples: vec![Sample {
                stack_index: 0,
                values: vec![7],
                ..Default::default()
            }],
            ..Default::default()
        };

        let decoded = super::otlp_profile_to_pprof(&profile, &dict).unwrap();
        let inner = decoded.inner();

        check!(
            inner.mapping
                == vec![
                    krabka_pprof::proto::Mapping {
                        id: 1,
                        memory_start: 0x10,
                        memory_limit: 0x20,
                        file_offset: 0x30,
                        filename: 9,
                        ..Default::default()
                    },
                    krabka_pprof::proto::Mapping {
                        id: 2,
                        memory_start: 0x40,
                        memory_limit: 0x50,
                        file_offset: 0x60,
                        filename: 10,
                        ..Default::default()
                    },
                ]
        );
        check!(
            inner.function
                == vec![
                    krabka_pprof::proto::Function {
                        id: 1,
                        name: 3,
                        system_name: 5,
                        filename: 7,
                        start_line: 11,
                    },
                    krabka_pprof::proto::Function {
                        id: 2,
                        name: 4,
                        system_name: 6,
                        filename: 8,
                        start_line: 22,
                    },
                ]
        );
        check!(
            inner.location
                == vec![
                    krabka_pprof::proto::Location {
                        id: 1,
                        mapping_id: 1,
                        address: 0x100,
                        line: vec![krabka_pprof::proto::Line {
                            function_id: 1,
                            line: 1,
                            column: 2
                        }],
                        ..Default::default()
                    },
                    krabka_pprof::proto::Location {
                        id: 2,
                        mapping_id: 2,
                        address: 0x200,
                        line: vec![krabka_pprof::proto::Line {
                            function_id: 2,
                            line: 3,
                            column: 4
                        }],
                        ..Default::default()
                    },
                ]
        );

        // Stack order is preserved as written, leaf first.
        check!(
            inner.sample
                == vec![krabka_pprof::proto::Sample {
                    location_id: vec![2, 1],
                    value: vec![7],
                    label: vec![],
                }]
        );

        check!(inner.time_nanos == 1_700_000_000_000_000_000);
        check!(inner.duration_nanos == 5_000);
        check!(inner.period == 99);
        check!(
            inner.sample_type == vec![krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            "sample type keeps type and unit in order"
        );
        check!(
            inner.period_type == Some(krabka_pprof::proto::ValueType { r#type: 2, unit: 1 }),
            "period type is not the sample type"
        );
    }

    /// Table indexes are zero-based, so the first invalid one is the length
    /// itself. That is the only value that separates a bounds check on `>=`
    /// from one on `>`, and getting it wrong yields an id one past the table
    /// rather than an error.
    #[test]
    fn a_table_index_equal_to_the_length_is_out_of_bounds() {
        use pb::otlp_profiles::{
            Function, Line, Location, Profile, ProfilesDictionary, Sample, Stack,
        };

        let dict = ProfilesDictionary {
            string_table: vec![String::new(), "fn_a".into()],
            function_table: vec![Function {
                name_strindex: 1,
                ..Default::default()
            }],
            location_table: vec![Location {
                lines: vec![Line {
                    function_index: 0,
                    line: 1,
                    column: 0,
                }],
                ..Default::default()
            }],
            // One location exists, so index 1 is the first one past the end.
            stack_table: vec![Stack {
                location_indices: vec![1],
            }],
            ..Default::default()
        };
        let profile = Profile {
            samples: vec![Sample {
                stack_index: 0,
                values: vec![1],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = super::otlp_profile_to_pprof(&profile, &dict)
            .unwrap_err()
            .to_string();
        check!(err.contains("references missing location"), "got: {err}");

        // A negative index cannot convert at all and is rejected the same way.
        let mut dict = dict;
        dict.stack_table = vec![Stack {
            location_indices: vec![-1],
        }];
        let err = super::otlp_profile_to_pprof(&profile, &dict)
            .unwrap_err()
            .to_string();
        check!(err.contains("references missing location"), "got: {err}");
    }

    /// The service name comes from the `service.name` resource attribute.
    /// Everything that is not a non-empty string there falls back to the
    /// placeholder, because a profile filed under an empty or absent name is
    /// unattributable.
    #[test]
    fn the_service_name_falls_back_whenever_it_is_not_a_usable_string() {
        use pb::opentelemetry::proto::{
            common::v1::{AnyValue, KeyValue, any_value::Value},
            resource::v1::Resource,
        };

        let with_attrs = |attrs: Vec<KeyValue>| pb::otlp_profiles::ResourceProfiles {
            resource: Some(Resource {
                attributes: attrs,
                ..Default::default()
            }),
            ..Default::default()
        };
        let attr = |key: &str, value: Option<Value>| KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value }),
        };

        check!(
            super::resolve_service_name(&with_attrs(vec![attr(
                "service.name",
                Some(Value::StringValue("payments".to_string()))
            )])) == "payments"
        );

        // The key is matched, not the position.
        check!(
            super::resolve_service_name(&with_attrs(vec![
                attr("other", Some(Value::StringValue("first".to_string()))),
                attr(
                    "service.name",
                    Some(Value::StringValue("payments".to_string()))
                ),
            ])) == "payments"
        );

        // Each way the attribute can be present but unusable.
        for (name, rp) in [
            (
                "no resource at all",
                pb::otlp_profiles::ResourceProfiles::default(),
            ),
            ("no attributes", with_attrs(vec![])),
            (
                "a different key",
                with_attrs(vec![attr(
                    "host.name",
                    Some(Value::StringValue("h".into())),
                )]),
            ),
            (
                "an empty name",
                with_attrs(vec![attr(
                    "service.name",
                    Some(Value::StringValue(String::new())),
                )]),
            ),
            (
                "a non-string value",
                with_attrs(vec![attr("service.name", Some(Value::IntValue(7)))]),
            ),
            ("no value", with_attrs(vec![attr("service.name", None)])),
        ] {
            check!(
                super::resolve_service_name(&rp) == "unknown_service",
                "{name} should fall back"
            );
        }
    }
    use assert2::{assert, check};

    use super::*;
    use crate::wire::pb;

    #[test]
    fn otlp_resolves_dictionary_into_rawprofile() {
        use pb::{
            opentelemetry::proto::common::v1::{AnyValue, any_value::Value},
            otlp_profiles::{
                Function, KeyValueAndUnit, Line, Link, Location, Profile, ProfilesDictionary,
                ResourceProfiles, Sample, ScopeProfiles, Stack, ValueType,
            },
        };

        let dict = ProfilesDictionary {
            string_table: vec![
                String::new(),
                "samples".into(),
                "count".into(),
                "main".into(),
                "target".into(),
                "all".into(),
                "env".into(),
            ],
            attribute_table: vec![
                KeyValueAndUnit {
                    key_strindex: 4,
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("all".to_string())),
                    }),
                    unit_strindex: 0,
                },
                KeyValueAndUnit {
                    key_strindex: 6,
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("prod".to_string())),
                    }),
                    unit_strindex: 0,
                },
            ],
            function_table: vec![Function {
                name_strindex: 3,
                ..Default::default()
            }],
            link_table: vec![Link {
                trace_id: vec![0xaa; 16],
                span_id: 42_u64.to_be_bytes().to_vec(),
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
                link_index: 0,
                attribute_indices: vec![0],
                values: vec![7],
                timestamps_unix_nano: vec![1_700_000_000_000_000_123],
            }],
            time_unix_nano: 1_700_000_000_000_000_000,
            attribute_indices: vec![1],
            profile_id: vec![0xab, 0xcd],
            ..Default::default()
        };
        let req = pb::otlp_profiles::ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                scope_profiles: vec![ScopeProfiles {
                    profiles: vec![profile],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            dictionary: Some(dict),
        };

        let out = decode_otlp(&req).unwrap();

        assert!(out.len() == 1);
        for (name, want) in [
            ("__name__", "samples"),
            ("env", "prod"),
            ("__profile_id__", "abcd"),
        ] {
            check!(out[0].labels.get(name) == Some(want));
        }
        check!(!out[0].profile.sample_types().is_empty());
        let split = crate::ingest::split_sample_types(&out[0]).unwrap();
        check!(split[0].samples[0].timestamp_ns == 1_700_000_000_000_000_123);
        check!(split[0].samples[0].span_id == Some(42));
        check!(split[0].samples[0].trace_id == Some(vec![0xaa; 16]));
        check!(split[0].labels.get("target") == Some("all"));
    }
}

// === split-modules: generated submodules ===
mod attribute_label;
mod decode_otlp;
mod hex_lower;
mod intern_string;
mod otlp_profile_to_pprof;
mod otlp_sample_links;
mod otlp_sample_timestamps;
mod profile_labels;
mod resolve_service_name;
mod sample_labels;
mod string_table;
mod table_ref;
mod table_ref_checked;
mod value_type;

use attribute_label::attribute_label;
pub use decode_otlp::decode_otlp;
use hex_lower::hex_lower;
use intern_string::intern_string;
use otlp_profile_to_pprof::otlp_profile_to_pprof;
use otlp_sample_links::otlp_sample_links;
use otlp_sample_timestamps::otlp_sample_timestamps;
use profile_labels::profile_labels;
use resolve_service_name::resolve_service_name;
use sample_labels::sample_labels;
use string_table::string_table;
use table_ref::table_ref;
use table_ref_checked::table_ref_checked;
use value_type::value_type;
