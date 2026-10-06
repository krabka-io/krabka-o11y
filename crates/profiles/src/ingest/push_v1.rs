//! Connect `push.v1.PusherService/Push` decode. Each `RawSample.raw_profile` is
//! a gzipped pprof. The steps are gunzip, then `PprofProfile::decode`, then one
//! `RawProfile` per sample.

use std::io::Read;

use krabka_blockstore::Labels;
use krabka_pprof::PprofProfile;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use crate::{error::ProfilesError, ingest::RawProfile, wire::pb};

#[cfg(test)]
mod tests {
    use std::io::Write;

    use assert2::assert;
    use krabka_units::{bytes, mebibytes};

    use super::*;
    use crate::wire::pb;

    fn gzip(raw: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(raw).unwrap();
        e.finish().unwrap()
    }

    #[test]
    fn gunzip_round_trips_and_caps() {
        let raw = b"the quick brown fox";
        let gz = gzip(raw);
        assert!(gunzip(&gz, mebibytes(1)).unwrap() == raw);
        assert!(gunzip(&gz, bytes(4)).is_err());
    }

    #[test]
    fn decode_push_gunzips_and_parses_pprof() {
        let pprof_bytes = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let req = pb::push::v1::PushRequest {
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
                    raw_profile: gzip(&pprof_bytes),
                    id: "s1".into(),
                }],
                annotations: Vec::new(),
            }],
        };

        let out = decode_push(&req, mebibytes(1)).unwrap();

        assert!(out.len() == 1);
        assert!(out[0].labels.get("__name__") == Some("process_cpu"));
    }

    #[test]
    fn decode_push_clears_only_symbolized_mapping_addresses() {
        use krabka_pprof::proto;
        let mut profile =
            PprofProfile::decode(&crate::wire::test_fixtures::cpu_profile_pprof_bytes())
                .unwrap()
                .into_inner();
        profile.mapping = vec![
            proto::Mapping {
                id: 1,
                memory_start: 10,
                memory_limit: 20,
                file_offset: 3,
                symbolization: proto::MappingSymbolization::from_parts((true, false, false, false)),
                ..Default::default()
            },
            proto::Mapping {
                id: 2,
                memory_start: 30,
                memory_limit: 40,
                file_offset: 5,
                ..Default::default()
            },
        ];
        profile.location = vec![
            proto::Location {
                id: 1,
                mapping_id: 1,
                address: 100,
                ..Default::default()
            },
            proto::Location {
                id: 2,
                mapping_id: 2,
                address: 200,
                ..Default::default()
            },
        ];
        let raw = PprofProfile::from(profile).encode();
        let request = pb::push::v1::PushRequest {
            series: vec![pb::push::v1::RawProfileSeries {
                samples: vec![pb::push::v1::RawSample {
                    raw_profile: gzip(&raw),
                    id: "sample".into(),
                }],
                ..Default::default()
            }],
        };
        let decoded = decode_push(&request, mebibytes(1)).unwrap();
        let profile = decoded[0].profile.inner();
        assert!(profile.location[0].address == 0);
        assert!(
            profile.mapping[0].memory_start == 0
                && profile.mapping[0].memory_limit == 0
                && profile.mapping[0].file_offset == 0
        );
        assert!(profile.location[1].address == 200);
        assert!(
            profile.mapping[1].memory_start == 30
                && profile.mapping[1].memory_limit == 40
                && profile.mapping[1].file_offset == 5
        );
    }

    #[test]
    fn decode_push_promotes_sample_id_to_profile_id_label() {
        let pprof_bytes = crate::wire::test_fixtures::cpu_profile_pprof_bytes();
        let req = pb::push::v1::PushRequest {
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
                    raw_profile: gzip(&pprof_bytes),
                    id: "profile-a".into(),
                }],
                annotations: Vec::new(),
            }],
        };

        let out = decode_push(&req, mebibytes(1)).unwrap();

        assert!(out.len() == 1);
        assert!(out[0].labels.get("__profile_id__") == Some("profile-a"));
    }
}

mod decode_push;
mod gunzip;

pub use decode_push::decode_push;
pub use gunzip::gunzip;
