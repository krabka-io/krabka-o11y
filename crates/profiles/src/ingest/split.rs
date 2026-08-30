//! Multi-value split: one pprof with N `sample_type[]` becomes N profile series.

use std::collections::{BTreeMap, HashMap};

use krabka_blockstore::Labels;
use krabka_pprof::ProfileType;

use crate::{
    error::ProfilesError,
    ingest::{DecodedProfile, DecodedSample, RawProfile},
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::Labels;
    use krabka_pprof::PprofProfile;

    use super::*;
    use crate::ingest::RawProfile;

    fn two_type_profile() -> PprofProfile {
        let profile = krabka_pprof::proto::Profile {
            sample_type: vec![
                krabka_pprof::proto::ValueType { r#type: 1, unit: 2 },
                krabka_pprof::proto::ValueType { r#type: 3, unit: 4 },
            ],
            sample: vec![krabka_pprof::proto::Sample {
                location_id: vec![7],
                value: vec![3, 4096],
                label: Vec::new(),
            }],
            location: (1..=7)
                .map(|id| krabka_pprof::proto::Location {
                    id,
                    ..Default::default()
                })
                .collect(),
            string_table: vec![
                String::new(),
                "alloc_objects".to_string(),
                "count".to_string(),
                "alloc_space".to_string(),
                "bytes".to_string(),
                "space".to_string(),
            ],
            period_type: Some(krabka_pprof::proto::ValueType { r#type: 5, unit: 4 }),
            time_nanos: 123_000_000,
            ..Default::default()
        };
        PprofProfile::from(profile)
    }

    #[test]
    fn split_yields_one_series_per_sample_type() {
        let mut labels = Labels::new();
        labels.insert("__name__", "memory");
        labels.insert("service_name", "api");
        let raw = RawProfile {
            labels,
            profile: two_type_profile(),
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
        };

        let out = split_sample_types(&raw).unwrap();
        assert!(out.len() == 2);

        let types: Vec<&str> = out
            .iter()
            .map(|profile| profile.profile_type.as_str())
            .collect();
        assert!(
            types
                .iter()
                .any(|profile_type| profile_type == &"memory:alloc_objects:count:space:bytes")
        );
        assert!(
            types
                .iter()
                .any(|profile_type| profile_type == &"memory:alloc_space:bytes:space:bytes")
        );

        let objects = out
            .iter()
            .find(|profile| profile.profile_type.contains("alloc_objects"))
            .unwrap();
        let space = out
            .iter()
            .find(|profile| profile.profile_type.contains("alloc_space"))
            .unwrap();

        check!(objects.samples[0].value == 3);
        check!(space.samples[0].value == 4096);
        check!(objects.samples[0].timestamp_ns == 123_000_000);
        check!(objects.samples[0].stacktrace_location_refs == vec![6]);
        for (name, want) in [
            ("__profile_type__", objects.profile_type.as_str()),
            ("__period_type__", "space"),
            ("__period_unit__", "bytes"),
            ("__type__", "alloc_objects"),
            ("__unit__", "count"),
            ("__service_name__", "api"),
        ] {
            check!(objects.labels.get(name) == Some(want));
        }
    }

    #[test]
    fn split_normalizes_pprof_location_ids_to_symbol_indices() {
        let profile = krabka_pprof::proto::Profile {
            sample_type: vec![krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![krabka_pprof::proto::Sample {
                location_id: vec![2],
                value: vec![5],
                label: Vec::new(),
            }],
            location: vec![
                krabka_pprof::proto::Location {
                    id: 1,
                    ..Default::default()
                },
                krabka_pprof::proto::Location {
                    id: 2,
                    ..Default::default()
                },
            ],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "sample".to_string(),
            ],
            period_type: Some(krabka_pprof::proto::ValueType { r#type: 3, unit: 2 }),
            ..Default::default()
        };
        let mut labels = Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");

        let out = split_sample_types(&RawProfile {
            labels,
            profile: PprofProfile::from(profile),
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
        })
        .unwrap();

        assert!(out[0].samples[0].stacktrace_location_refs == vec![1]);
    }

    #[test]
    fn split_promotes_pprof_string_sample_labels_to_series_labels() {
        let profile = krabka_pprof::proto::Profile {
            sample_type: vec![krabka_pprof::proto::ValueType { r#type: 1, unit: 2 }],
            sample: vec![
                krabka_pprof::proto::Sample {
                    location_id: vec![1],
                    value: vec![5],
                    label: vec![krabka_pprof::proto::Label {
                        key: 4,
                        str: 5,
                        num: 0,
                        num_unit: 0,
                    }],
                },
                krabka_pprof::proto::Sample {
                    location_id: vec![1],
                    value: vec![7],
                    label: vec![krabka_pprof::proto::Label {
                        key: 4,
                        str: 6,
                        num: 0,
                        num_unit: 0,
                    }],
                },
            ],
            location: vec![krabka_pprof::proto::Location {
                id: 1,
                ..Default::default()
            }],
            string_table: vec![
                String::new(),
                "samples".to_string(),
                "count".to_string(),
                "sample".to_string(),
                "target".to_string(),
                "all".to_string(),
                "self".to_string(),
            ],
            period_type: Some(krabka_pprof::proto::ValueType { r#type: 3, unit: 2 }),
            ..Default::default()
        };
        let mut labels = Labels::new();
        labels.insert("__name__", "samples");
        labels.insert("service_name", "api");

        let out = split_sample_types(&RawProfile {
            labels,
            profile: PprofProfile::from(profile),
            delta: false,
            sample_timestamps_ns: Vec::new(),
            sample_span_ids: Vec::new(),
            sample_trace_ids: Vec::new(),
        })
        .unwrap();

        check!(out.len() == 2);
        for target in ["all", "self"] {
            check!(
                out.iter()
                    .any(|profile| profile.labels.get("target") == Some(target))
            );
        }
    }
}

mod labels_key;
mod labels_with_sample_labels;
mod split_sample_types;

use labels_key::labels_key;
use labels_with_sample_labels::labels_with_sample_labels;
pub use split_sample_types::split_sample_types;
