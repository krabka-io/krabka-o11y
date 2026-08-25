//! OTLP `v1development` profiles -> `Vec<RawProfile>`.
//!
//! The generated OTLP types live in this crate, so the edge converts them into
//! the pprof wire model owned by `crabka-pprof`.

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;

use crate::{error::ProfilesError, ingest::RawProfile, wire::pb};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn decode_otlp(
    req: &pb::otlp_profiles::ExportProfilesServiceRequest,
) -> Result<Vec<RawProfile>, ProfilesError> {
    let dict = req
        .dictionary
        .as_ref()
        .ok_or_else(|| ProfilesError::Invalid("OTLP profiles missing dictionary".to_string()))?;
    let mut out = Vec::new();

    for resource_profiles in &req.resource_profiles {
        let service_name = resolve_service_name(resource_profiles);
        for scope_profiles in &resource_profiles.scope_profiles {
            for profile in &scope_profiles.profiles {
                let sample_timestamps_ns = otlp_sample_timestamps(profile)?;
                let (sample_span_ids, sample_trace_ids) = otlp_sample_links(profile, dict)?;
                let profile_labels = profile_labels(profile, dict)?;
                let profile_id =
                    (!profile.profile_id.is_empty()).then(|| hex_lower(&profile.profile_id));
                let profile = otlp_profile_to_pprof(profile, dict)?;
                let mut labels = Labels::new();
                labels.insert("service_name", service_name.clone());
                if let Some(profile_id) = profile_id {
                    labels.insert("__profile_id__", profile_id);
                }
                for (name, value) in profile_labels {
                    labels.insert(name, value);
                }
                if let Some((name, _)) = profile.sample_types().first() {
                    labels.insert("__name__", name.clone());
                }
                out.push(RawProfile {
                    labels,
                    profile,
                    delta: false,
                    sample_timestamps_ns,
                    sample_span_ids,
                    sample_trace_ids,
                });
            }
        }
    }

    Ok(out)
}

fn otlp_profile_to_pprof(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<PprofProfile, ProfilesError> {
    let mut pprof = crabka_pprof::proto::Profile {
        string_table: string_table(dict),
        mapping: dict
            .mapping_table
            .iter()
            .enumerate()
            .map(|(idx, mapping)| crabka_pprof::proto::Mapping {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                memory_start: mapping.memory_start,
                memory_limit: mapping.memory_limit,
                file_offset: mapping.file_offset,
                filename: i64::from(mapping.filename_strindex),
                ..Default::default()
            })
            .collect(),
        function: dict
            .function_table
            .iter()
            .enumerate()
            .map(|(idx, function)| crabka_pprof::proto::Function {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                name: i64::from(function.name_strindex),
                system_name: i64::from(function.system_name_strindex),
                filename: i64::from(function.filename_strindex),
                start_line: function.start_line,
            })
            .collect(),
        location: dict
            .location_table
            .iter()
            .enumerate()
            .map(|(idx, location)| crabka_pprof::proto::Location {
                id: u64::try_from(idx + 1).unwrap_or(u64::MAX),
                mapping_id: table_ref(location.mapping_index, dict.mapping_table.len()),
                address: location.address,
                line: location
                    .lines
                    .iter()
                    .map(|line| crabka_pprof::proto::Line {
                        function_id: table_ref(line.function_index, dict.function_table.len()),
                        line: line.line,
                        column: line.column,
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        time_nanos: i64::try_from(profile.time_unix_nano)
            .map_err(|_| ProfilesError::Invalid("OTLP profile time overflows i64".to_string()))?,
        duration_nanos: i64::try_from(profile.duration_nano).map_err(|_| {
            ProfilesError::Invalid("OTLP profile duration overflows i64".to_string())
        })?,
        period: profile.period,
        ..Default::default()
    };

    if let Some(sample_type) = &profile.sample_type {
        pprof.sample_type.push(value_type(*sample_type));
    }
    if let Some(period_type) = &profile.period_type {
        pprof.period_type = Some(value_type(*period_type));
    }

    for sample in &profile.samples {
        let stack = usize::try_from(sample.stack_index)
            .ok()
            .and_then(|idx| dict.stack_table.get(idx))
            .ok_or_else(|| ProfilesError::Invalid("OTLP sample references missing stack".into()))?;
        let location_id = stack
            .location_indices
            .iter()
            .map(|idx| {
                table_ref_checked(
                    *idx,
                    dict.location_table.len(),
                    "OTLP stack references missing location",
                )
            })
            .collect::<Result<Vec<_>, ProfilesError>>()?;
        pprof.sample.push(crabka_pprof::proto::Sample {
            location_id,
            value: sample.values.clone(),
            label: sample_labels(sample, dict, &mut pprof.string_table)?,
        });
    }

    Ok(PprofProfile::from(pprof))
}

fn profile_labels(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<Vec<(String, String)>, ProfilesError> {
    profile
        .attribute_indices
        .iter()
        .map(|idx| attribute_label(*idx, dict))
        .collect()
}

fn sample_labels(
    sample: &pb::otlp_profiles::Sample,
    dict: &pb::otlp_profiles::ProfilesDictionary,
    strings: &mut Vec<String>,
) -> Result<Vec<crabka_pprof::proto::Label>, ProfilesError> {
    let mut labels = Vec::new();
    for attr_idx in &sample.attribute_indices {
        let (name, value) = attribute_label(*attr_idx, dict)?;
        let key = intern_string(strings, &name);
        let value_idx = intern_string(strings, &value);
        labels.push(crabka_pprof::proto::Label {
            key,
            str: value_idx,
            num: 0,
            num_unit: 0,
        });
    }
    Ok(labels)
}

fn attribute_label(
    index: i32,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<(String, String), ProfilesError> {
    use pb::opentelemetry::proto::common::v1::any_value::Value;

    let attr = usize::try_from(index)
        .ok()
        .and_then(|idx| dict.attribute_table.get(idx))
        .ok_or_else(|| ProfilesError::Invalid("OTLP references missing attribute".into()))?;
    let key_idx = usize::try_from(attr.key_strindex).map_err(|_| {
        ProfilesError::Invalid("OTLP attribute key references missing string".to_string())
    })?;
    let key = dict
        .string_table
        .get(key_idx)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ProfilesError::Invalid("OTLP attribute key references missing string".to_string())
        })?
        .clone();
    let value = match attr.value.as_ref().and_then(|value| value.value.as_ref()) {
        Some(Value::StringValue(value)) => value.clone(),
        Some(Value::IntValue(value)) => value.to_string(),
        None => String::new(),
    };
    Ok((key, value))
}

fn intern_string(strings: &mut Vec<String>, value: &str) -> i64 {
    if let Some(idx) = strings.iter().position(|existing| existing == value) {
        return i64::try_from(idx).expect("string index fits i64");
    }
    let idx = i64::try_from(strings.len()).expect("string index fits i64");
    strings.push(value.to_string());
    idx
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(HEX[usize::from(byte >> 4)]));
        out.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    out
}

fn otlp_sample_timestamps(
    profile: &pb::otlp_profiles::Profile,
) -> Result<Vec<Vec<i64>>, ProfilesError> {
    profile
        .samples
        .iter()
        .map(|sample| {
            sample
                .timestamps_unix_nano
                .iter()
                .map(|timestamp| {
                    i64::try_from(*timestamp).map_err(|_| {
                        ProfilesError::Invalid("OTLP sample timestamp overflows i64".to_string())
                    })
                })
                .collect()
        })
        .collect()
}

type OtlpSampleLinks = (Vec<Option<u64>>, Vec<Option<Vec<u8>>>);

fn otlp_sample_links(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<OtlpSampleLinks, ProfilesError> {
    let mut span_ids = Vec::with_capacity(profile.samples.len());
    let mut trace_ids = Vec::with_capacity(profile.samples.len());
    for sample in &profile.samples {
        if dict.link_table.is_empty() {
            span_ids.push(None);
            trace_ids.push(None);
            continue;
        }
        let link = usize::try_from(sample.link_index)
            .ok()
            .and_then(|idx| dict.link_table.get(idx))
            .ok_or_else(|| ProfilesError::Invalid("OTLP sample references missing link".into()))?;
        let span_id = if link.span_id.is_empty() {
            None
        } else {
            let bytes: [u8; 8] = link.span_id.as_slice().try_into().map_err(|_| {
                ProfilesError::Invalid("OTLP link span_id must be 8 bytes".to_string())
            })?;
            Some(u64::from_be_bytes(bytes))
        };
        let trace_id = (!link.trace_id.is_empty()).then(|| link.trace_id.clone());
        span_ids.push(span_id);
        trace_ids.push(trace_id);
    }
    Ok((span_ids, trace_ids))
}

fn value_type(value: pb::otlp_profiles::ValueType) -> crabka_pprof::proto::ValueType {
    crabka_pprof::proto::ValueType {
        r#type: i64::from(value.type_strindex),
        unit: i64::from(value.unit_strindex),
    }
}

fn string_table(dict: &pb::otlp_profiles::ProfilesDictionary) -> Vec<String> {
    if dict.string_table.is_empty() {
        vec![String::new()]
    } else {
        dict.string_table.clone()
    }
}

fn table_ref(index: i32, len: usize) -> u64 {
    table_ref_checked(index, len, "").unwrap_or(0)
}

fn table_ref_checked(index: i32, len: usize, message: &str) -> Result<u64, ProfilesError> {
    let idx = usize::try_from(index).map_err(|_| ProfilesError::Invalid(message.to_string()))?;
    if idx >= len {
        return Err(ProfilesError::Invalid(message.to_string()));
    }
    Ok(u64::try_from(idx + 1).unwrap_or(u64::MAX))
}

fn resolve_service_name(rp: &pb::otlp_profiles::ResourceProfiles) -> String {
    use pb::opentelemetry::proto::common::v1::any_value::Value;

    let Some(resource) = &rp.resource else {
        return "unknown_service".to_string();
    };
    for attr in &resource.attributes {
        if attr.key == "service.name"
            && let Some(value) = &attr.value
            && let Some(Value::StringValue(service)) = &value.value
            && !service.is_empty()
        {
            return service.clone();
        }
    }
    "unknown_service".to_string()
}

#[cfg(test)]
mod tests {

    /// `resolve_service_name` reads `service.name` from the resource, and
    /// falls back to a fixed placeholder for every way that can fail. Each way
    /// is checked separately, since they reach the fallback by different
    /// routes and a guard removed from one is invisible to the others.
    #[test]
    fn a_missing_service_name_falls_back_rather_than_erroring() {
        use pb::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value::Value};
        use pb::opentelemetry::proto::resource::v1::Resource;

        let with_attrs = |attrs: Vec<KeyValue>| pb::otlp_profiles::ResourceProfiles {
            resource: Some(Resource { attributes: attrs, ..Resource::default() }),
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
            string_table: ["", "samples", "count", "fn_a", "fn_b", "sys_a", "sys_b",
            //             7        8        9        10
                           "file_a", "file_b", "map_a", "map_b"]
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
                    lines: vec![Line { function_index: 0, line: 1, column: 2 }],
                    ..Default::default()
                },
                // References the *second* mapping and function, so a
                // renumbering that is off by one lands somewhere visible.
                Location {
                    mapping_index: 1,
                    address: 0x200,
                    lines: vec![Line { function_index: 1, line: 3, column: 4 }],
                    ..Default::default()
                },
            ],
            stack_table: vec![Stack { location_indices: vec![1, 0] }],
            ..Default::default()
        };

        let profile = Profile {
            sample_type: Some(ValueType { type_strindex: 1, unit_strindex: 2 }),
            period_type: Some(ValueType { type_strindex: 2, unit_strindex: 1 }),
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
                    crabka_pprof::proto::Mapping {
                        id: 1,
                        memory_start: 0x10,
                        memory_limit: 0x20,
                        file_offset: 0x30,
                        filename: 9,
                        ..Default::default()
                    },
                    crabka_pprof::proto::Mapping {
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
                    crabka_pprof::proto::Function {
                        id: 1,
                        name: 3,
                        system_name: 5,
                        filename: 7,
                        start_line: 11,
                    },
                    crabka_pprof::proto::Function {
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
                    crabka_pprof::proto::Location {
                        id: 1,
                        mapping_id: 1,
                        address: 0x100,
                        line: vec![crabka_pprof::proto::Line { function_id: 1, line: 1, column: 2 }],
                        ..Default::default()
                    },
                    crabka_pprof::proto::Location {
                        id: 2,
                        mapping_id: 2,
                        address: 0x200,
                        line: vec![crabka_pprof::proto::Line { function_id: 2, line: 3, column: 4 }],
                        ..Default::default()
                    },
                ]
        );

        // Stack order is preserved as written, leaf first.
        check!(
            inner.sample
                == vec![crabka_pprof::proto::Sample {
                    location_id: vec![2, 1],
                    value: vec![7],
                    label: vec![],
                }]
        );

        check!(inner.time_nanos == 1_700_000_000_000_000_000);
        check!(inner.duration_nanos == 5_000);
        check!(inner.period == 99);
        check!(
            inner.sample_type
                == vec![crabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            "sample type keeps type and unit in order"
        );
        check!(
            inner.period_type
                == Some(crabka_pprof::proto::ValueType { r#type: 2, unit: 1 }),
            "period type is not the sample type"
        );
    }

    /// Table indexes are zero-based, so the first invalid one is the length
    /// itself. That is the only value that separates a bounds check on `>=`
    /// from one on `>`, and getting it wrong yields an id one past the table
    /// rather than an error.
    #[test]
    fn a_table_index_equal_to_the_length_is_out_of_bounds() {
        use pb::otlp_profiles::{Function, Line, Location, Profile, ProfilesDictionary, Sample, Stack};

        let dict = ProfilesDictionary {
            string_table: vec![String::new(), "fn_a".into()],
            function_table: vec![Function { name_strindex: 1, ..Default::default() }],
            location_table: vec![Location {
                lines: vec![Line { function_index: 0, line: 1, column: 0 }],
                ..Default::default()
            }],
            // One location exists, so index 1 is the first one past the end.
            stack_table: vec![Stack { location_indices: vec![1] }],
            ..Default::default()
        };
        let profile = Profile {
            samples: vec![Sample { stack_index: 0, values: vec![1], ..Default::default() }],
            ..Default::default()
        };

        let err = super::otlp_profile_to_pprof(&profile, &dict)
            .unwrap_err()
            .to_string();
        check!(err.contains("references missing location"), "got: {err}");

        // A negative index cannot convert at all and is rejected the same way.
        let mut dict = dict;
        dict.stack_table = vec![Stack { location_indices: vec![-1] }];
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
        use pb::opentelemetry::proto::common::v1::{AnyValue, KeyValue, any_value::Value};
        use pb::opentelemetry::proto::resource::v1::Resource;

        let with_attrs = |attrs: Vec<KeyValue>| pb::otlp_profiles::ResourceProfiles {
            resource: Some(Resource { attributes: attrs, ..Default::default() }),
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
                attr("service.name", Some(Value::StringValue("payments".to_string()))),
            ])) == "payments"
        );

        // Each way the attribute can be present but unusable.
        for (name, rp) in [
            ("no resource at all", pb::otlp_profiles::ResourceProfiles::default()),
            ("no attributes", with_attrs(vec![])),
            (
                "a different key",
                with_attrs(vec![attr("host.name", Some(Value::StringValue("h".into())))]),
            ),
            (
                "an empty name",
                with_attrs(vec![attr("service.name", Some(Value::StringValue(String::new())))]),
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
