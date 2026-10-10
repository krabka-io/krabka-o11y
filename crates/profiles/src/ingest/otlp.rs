//! OTLP `v1development` profiles -> `Vec<RawProfile>`.
//!
//! The generated OTLP types live in this crate, so the edge converts them into
//! the pprof wire model owned by `krabka-pprof`.

use krabka_blockstore::{Labels, span_id_u64_from_be_slice};
use krabka_pprof::PprofProfile;

use crate::{error::ProfilesError, ingest::RawProfile, wire::pb};

#[cfg(test)]
mod tests {

    /// A resource-profiles message whose resource carries `attributes`.
    fn resource_profiles_with(
        attributes: Vec<pb::opentelemetry::proto::common::v1::KeyValue>,
    ) -> pb::otlp_profiles::ResourceProfiles {
        pb::otlp_profiles::ResourceProfiles {
            resource: Some(pb::opentelemetry::proto::resource::v1::Resource {
                attributes,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// A resource attribute named `key` holding `value`, or holding an
    /// `AnyValue` with nothing in it when `value` is `None`.
    fn attribute(
        key: &str,
        value: Option<pb::opentelemetry::proto::common::v1::any_value::Value>,
    ) -> pb::opentelemetry::proto::common::v1::KeyValue {
        use pb::opentelemetry::proto::common::v1::{AnyValue, KeyValue};

        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value }),
        }
    }

    /// A resource attribute named `key` holding the string `value`.
    fn string_attribute(key: &str, value: &str) -> pb::opentelemetry::proto::common::v1::KeyValue {
        use pb::opentelemetry::proto::common::v1::any_value::Value;

        attribute(key, Some(Value::StringValue(value.to_string())))
    }

    /// The service name comes from the `service.name` resource attribute, and
    /// is returned as written. The key is matched, not the position.
    #[test]
    fn the_service_name_is_read_from_its_resource_attribute() {
        for (case, attributes, expected) in [
            (
                "the only attribute",
                vec![string_attribute("service.name", "checkout")],
                "checkout",
            ),
            (
                "after another attribute",
                vec![
                    string_attribute("host.name", "box"),
                    string_attribute("service.name", "checkout"),
                ],
                "checkout",
            ),
            (
                "the only attribute, another name",
                vec![string_attribute("service.name", "payments")],
                "payments",
            ),
            (
                "after an attribute with another key",
                vec![
                    string_attribute("other", "first"),
                    string_attribute("service.name", "payments"),
                ],
                "payments",
            ),
        ] {
            check!(
                super::resolve_service_name(&resource_profiles_with(attributes)) == expected,
                "{case}"
            );
        }
    }

    /// `resolve_service_name` falls back to a fixed placeholder for every way
    /// reading `service.name` from the resource can fail. Each way is checked
    /// separately, since they reach the fallback by different routes and a
    /// guard removed from one is invisible to the others. A profile filed
    /// under an empty or absent name is unattributable, so everything that is
    /// not a non-empty string falls back.
    #[test]
    fn a_missing_service_name_falls_back_rather_than_erroring() {
        use pb::opentelemetry::proto::common::v1::any_value::Value;

        for (case, resource_profiles) in [
            (
                "no resource at all",
                pb::otlp_profiles::ResourceProfiles::default(),
            ),
            (
                "a resource with no attributes",
                resource_profiles_with(Vec::new()),
            ),
            (
                "the wrong key",
                resource_profiles_with(vec![string_attribute("host.name", "box")]),
            ),
            (
                "a different key",
                resource_profiles_with(vec![string_attribute("host.name", "h")]),
            ),
            (
                "the key with no value",
                resource_profiles_with(vec![attribute("service.name", None)]),
            ),
            (
                "a value that is not a string",
                resource_profiles_with(vec![attribute("service.name", Some(Value::IntValue(7)))]),
            ),
            (
                "an empty name is not a name",
                resource_profiles_with(vec![string_attribute("service.name", "")]),
            ),
        ] {
            check!(
                super::resolve_service_name(&resource_profiles) == "unknown_service",
                "{case} should fall back"
            );
        }
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

    use assert2::{assert, check};

    use super::*;
    use crate::{
        test_support::{otlp_single_frame_dictionary, otlp_value_type},
        wire::pb,
    };

    #[test]
    fn otlp_resolves_dictionary_into_rawprofile() {
        use pb::{
            opentelemetry::proto::{
                common::v1::{AnyValue, KeyValue, any_value::Value},
                resource::v1::Resource,
            },
            otlp_profiles::{
                KeyValueAndUnit, Link, Profile, ProfilesDictionary, ResourceProfiles, Sample,
                ScopeProfiles,
            },
        };

        let dict = ProfilesDictionary {
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
            link_table: vec![Link {
                trace_id: vec![0xaa; 16],
                span_id: 42_u64.to_be_bytes().to_vec(),
            }],
            ..otlp_single_frame_dictionary(vec![
                String::new(),
                "samples".into(),
                "count".into(),
                "main".into(),
                "target".into(),
                "all".into(),
                "env".into(),
            ])
        };
        let profile = Profile {
            sample_type: Some(otlp_value_type()),
            period_type: Some(otlp_value_type()),
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
                resource: Some(Resource {
                    attributes: vec![
                        KeyValue {
                            key: "process.pid".into(),
                            value: Some(AnyValue {
                                value: Some(Value::IntValue(42)),
                            }),
                        },
                        KeyValue {
                            key: "process.executable.name".into(),
                            value: Some(AnyValue {
                                value: Some(Value::StringValue("worker".into())),
                            }),
                        },
                    ],
                    ..Default::default()
                }),
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
            ("process.pid", "42"),
            ("process.executable.name", "worker"),
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

mod attribute_label;
mod decode_otlp;
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
use krabka_blockstore::encode_lower_hex;
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
