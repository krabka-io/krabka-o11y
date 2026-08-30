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

// === split-modules: generated submodules ===
mod decode_push;
mod gunzip;

pub use decode_push::decode_push;
pub use gunzip::gunzip;
