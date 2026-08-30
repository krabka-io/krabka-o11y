//! Decode targets and pre-WAL ingest pipeline helpers.
//!
//! Push doors lower into these types. The distributor then applies relabeling,
//! required labels, structural limits, and the `__session_id__` cardinality cap
//! before it writes to the profile WAL.

pub mod legacy;
pub mod otlp;
pub mod push_v1;
pub mod split;

use std::collections::BTreeMap;

use krabka_blockstore::Labels;
use krabka_pprof::PprofProfile;
use krabka_units::{ByteSize, bytes, convert::ByteSizeExt as _};
pub use legacy::{
    IngestFormat, IngestQuery, LegacyDecodeLimits, decode_ingest_body,
    decode_ingest_body_with_limits, decode_ingest_multipart, decode_ingest_multipart_with_limits,
    parse_ingest_query,
};
pub use otlp::decode_otlp;
pub use push_v1::{decode_push, gunzip};
use serde::{Deserialize, Serialize};
pub use split::split_sample_types;

use crate::error::ProfilesError;

mod label_byte_limit {
    use krabka_units::{ByteSize, convert::ByteSizeExt as _};
    use serde::{Deserializer, Serializer, de::Error as _};

    #[cfg(target_pointer_width = "64")]
    const USIZE_UPPER_EXCLUSIVE: f64 = 18_446_744_073_709_551_616.0;
    #[cfg(target_pointer_width = "32")]
    const USIZE_UPPER_EXCLUSIVE: f64 = 4_294_967_296.0;

    #[allow(clippy::trivially_copy_pass_by_ref)] // Required by serde's `with` adapter contract.
    pub fn serialize<S: Serializer>(value: &ByteSize, serializer: S) -> Result<S::Ok, S::Error> {
        krabka_units::serde_units::human::byte_size::serialize(value, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ByteSize, D::Error> {
        let value = krabka_units::serde_units::human::byte_size::deserialize(deserializer)?;
        let bytes = value.bytes_f64();
        if bytes >= 0.0 && bytes.fract() == 0.0 && bytes < USIZE_UPPER_EXCLUSIVE {
            Ok(value)
        } else {
            Err(D::Error::custom(
                "label byte limit must be a non-negative whole byte count representable by usize",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    /// `apply_relabel` returns whether the series survives, and rewrites its
    /// labels on the way. Each action is checked on both a matching and a
    /// non-matching config, since every one of them is a no-op on one side and
    /// the interesting case only appears on the other.
    #[test]
    fn relabelling_drops_keeps_and_rewrites_by_action() {
        use super::RelabelAction::{Drop, Keep, Replace};

        let config = |action, sources: &[&str], regex: &str, target: &str, replacement: &str| {
            super::RelabelConfig {
                source_labels: sources.iter().map(|s| (*s).to_string()).collect(),
                regex: regex.to_string(),
                target_label: target.to_string(),
                replacement: replacement.to_string(),
                action,
            }
        };
        let labels = || {
            let mut set = Labels::new();
            set.insert("app", "web");
            set.insert("env", "prod");
            set
        };

        // Drop removes the series when it matches, and leaves it when it does not.
        let mut set = labels();
        check!(!super::apply_relabel(
            &mut set,
            &[config(Drop, &["app"], "web", "", "")]
        ));
        let mut set = labels();
        check!(super::apply_relabel(
            &mut set,
            &[config(Drop, &["app"], "api", "", "")]
        ));

        // Keep is the mirror: it removes the series when it does *not* match.
        let mut set = labels();
        check!(super::apply_relabel(
            &mut set,
            &[config(Keep, &["app"], "web", "", "")]
        ));
        let mut set = labels();
        check!(!super::apply_relabel(
            &mut set,
            &[config(Keep, &["app"], "api", "", "")]
        ));

        // Replace writes the target label when it matches, and not otherwise.
        let mut set = labels();
        check!(super::apply_relabel(
            &mut set,
            &[config(Replace, &["app"], "web", "tier", "front")]
        ));
        check!(set.get("tier") == Some("front"));
        let mut set = labels();
        check!(super::apply_relabel(
            &mut set,
            &[config(Replace, &["app"], "api", "tier", "front")]
        ));
        check!(set.get("tier").is_none(), "no match, no write");

        // An empty replacement removes the target rather than setting it empty.
        let mut set = labels();
        check!(super::apply_relabel(
            &mut set,
            &[config(Replace, &["app"], "web", "env", "")]
        ));
        check!(set.get("env").is_none(), "removed, not blanked");

        // The regex is anchored at both ends, so neither a prefix nor a
        // suffix of the value matches. Each end needs its own case: a pattern
        // matching neither end is rejected however the anchors are written.
        let mut set = labels();
        check!(
            super::apply_relabel(&mut set, &[config(Drop, &["app"], "we", "", "")]),
            "a prefix must not match"
        );
        let mut set = labels();
        check!(
            super::apply_relabel(&mut set, &[config(Drop, &["app"], "eb", "", "")]),
            "nor a suffix"
        );

        // Source labels join with a separator, so where they divide matters.
        let mut set = labels();
        check!(
            !super::apply_relabel(
                &mut set,
                &[config(Drop, &["app", "env"], "web;prod", "", "")]
            ),
            "the joined value matches"
        );
        let mut set = labels();
        check!(
            super::apply_relabel(
                &mut set,
                &[config(Drop, &["app", "env"], "webprod", "", "")]
            ),
            "and does not match without the separator"
        );

        // A missing source label reads as empty rather than skipping the join.
        let mut set = labels();
        check!(
            !super::apply_relabel(
                &mut set,
                &[config(Drop, &["app", "absent"], "web;", "", "")]
            ),
            "the absent label contributes nothing but its separator"
        );

        // A config whose regex does not compile is skipped rather than taken
        // as a match. On its own it must leave the series alone -- with a
        // dropping config after it, both a skip and a drop would return false
        // and the two could not be told apart.
        let mut set = labels();
        check!(
            super::apply_relabel(&mut set, &[config(Drop, &["app"], "(unclosed", "", "")]),
            "an uncompilable regex drops nothing"
        );

        // And the configs after it still apply.
        let mut set = labels();
        check!(!super::apply_relabel(
            &mut set,
            &[
                config(Drop, &["app"], "(unclosed", "", ""),
                config(Drop, &["app"], "web", "", ""),
            ]
        ));

        // With no configs at all the series survives untouched.
        let mut set = labels();
        check!(super::apply_relabel(&mut set, &[]));
        check!(set.get("app") == Some("web"));
    }
    use assert2::{assert, check};
    use krabka_blockstore::Labels;

    use super::*;

    /// FNV-1a, checked against the reference vectors rather than against
    /// values this implementation produced. The constants and the
    /// xor-then-multiply order are the whole algorithm, and a version that
    /// multiplies before xoring, or seeds from zero, still looks like a hash.
    #[test]
    fn fnv1a_matches_the_published_vectors() {
        let hash = |s: &str| super::fnv1a(s.as_bytes());

        assert!(
            hash("") == 0xcbf2_9ce4_8422_2325,
            "the empty input is the offset basis"
        );
        assert!(hash("a") == 0xaf63_dc4c_8601_ec8c);
        assert!(hash("b") == 0xaf63_df4c_8601_f1a5);
        assert!(hash("c") == 0xaf63_de4c_8601_eff2);
        assert!(hash("foobar") == 0x8594_4171_f739_67e8);

        // Order matters, so a hash that folded bytes commutatively would not
        // pass even if every vector above did.
        assert!(hash("ab") != hash("ba"));
    }

    fn relabel(action: RelabelAction, sources: &[&str], regex: &str) -> RelabelConfig {
        RelabelConfig {
            source_labels: sources.iter().map(|s| (*s).to_string()).collect(),
            regex: regex.to_string(),
            target_label: "target".to_string(),
            replacement: "new".to_string(),
            action,
        }
    }

    /// `apply_relabel` returns whether the series survives. Drop and Keep are
    /// mirror images -- one rejects on a match, the other on the absence of
    /// one -- so both are checked from both sides.
    #[test]
    fn relabel_drop_and_keep_are_mirror_images() {
        let mut series = labels(&[("env", "prod")]);

        assert!(!apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Drop, &["env"], "prod")]
        ));
        assert!(apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Drop, &["env"], "dev")]
        ));
        assert!(apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Keep, &["env"], "prod")]
        ));
        assert!(!apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Keep, &["env"], "dev")]
        ));

        // The regex is anchored, so a partial match is not a match.
        assert!(apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Drop, &["env"], "pro")]
        ));

        // A label that is not set reads as empty rather than skipping the rule.
        assert!(!apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Keep, &["absent"], "prod")]
        ));
        assert!(apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Keep, &["absent"], "")]
        ));

        // Several source labels are joined with ';' before matching.
        let mut two = labels(&[("a", "x"), ("b", "y")]);
        assert!(!apply_relabel(
            &mut two,
            &[relabel(RelabelAction::Drop, &["a", "b"], "x;y")]
        ));
        assert!(apply_relabel(
            &mut two,
            &[relabel(RelabelAction::Drop, &["a", "b"], "xy")]
        ));

        // A rule whose regex will not compile is skipped, not treated as a
        // match: one bad rule must not drop every series.
        assert!(apply_relabel(
            &mut series,
            &[relabel(RelabelAction::Keep, &["env"], "[")]
        ));
    }

    /// A Replace rule with an empty replacement removes the target label
    /// instead of setting it to the empty string, and touches nothing else.
    #[test]
    fn relabel_replace_sets_or_removes_only_the_target() {
        let mut series = labels(&[("env", "prod"), ("target", "old"), ("keep", "me")]);
        let mut config = relabel(RelabelAction::Replace, &["env"], "prod");

        assert!(apply_relabel(&mut series, std::slice::from_ref(&config)));
        assert!(series == labels(&[("env", "prod"), ("target", "new"), ("keep", "me")]));

        config.replacement = String::new();
        assert!(apply_relabel(&mut series, std::slice::from_ref(&config)));
        assert!(series == labels(&[("env", "prod"), ("keep", "me")]));

        // A rule that does not match leaves the labels alone.
        config.replacement = "other".to_string();
        config.regex = "dev".to_string();
        assert!(apply_relabel(&mut series, std::slice::from_ref(&config)));
        assert!(series == labels(&[("env", "prod"), ("keep", "me")]));
    }

    /// Each ingest limit rejects what exceeds it, so a series sitting exactly
    /// on every limit is still admitted. All three are checked at their edge
    /// and one past it.
    #[test]
    fn ingest_limits_admit_exactly_their_boundary() {
        let limits = TenantLimits {
            max_label_name: krabka_units::bytes(3),
            max_label_names_per_series: 2,
            max_label_value: krabka_units::bytes(4),
            session_id_buckets: 1,
        };

        // Two labels, a three-byte name and a four-byte value: all at the edge.
        let at_edge = labels(&[("abc", "wxyz"), ("de", "fg")]);
        assert!(enforce_limits(&at_edge, &limits).is_ok());

        let too_many = labels(&[("a", "b"), ("c", "d"), ("e", "f")]);
        let err = enforce_limits(&too_many, &limits).unwrap_err().to_string();
        assert!(err.contains("too many label names: 3 > 2"));

        let long_name = labels(&[("abcd", "x")]);
        let err = enforce_limits(&long_name, &limits).unwrap_err().to_string();
        assert!(err.contains("`abcd` name exceeds 3 bytes"));

        let long_value = labels(&[("a", "wxyz!")]);
        let err = enforce_limits(&long_value, &limits)
            .unwrap_err()
            .to_string();
        assert!(err.contains("`a` value exceeds 4 bytes"));
    }

    /// A label byte limit is a count of bytes, so it has to be a whole
    /// non-negative number that a usize can hold. The deserializer rejects
    /// each way that can fail, and rejecting is the point: a limit that
    /// silently rounded or wrapped would be enforced as something other than
    /// what was configured.
    #[test]
    fn a_label_byte_limit_must_be_a_whole_non_negative_count() {
        let parse = |limit: &str| {
            let json = String::from("{\"max_label_name\":\"")
                + limit
                + "\",\"max_label_names_per_series\":2,\"max_label_value\":\"4B\",\"session_id_buckets\":1}";
            serde_json::from_str::<TenantLimits>(&json).map(|limits| limits.max_label_name)
        };

        assert!(
            parse("0B").unwrap() == krabka_units::bytes(0),
            "zero is a limit"
        );
        assert!(parse("3B").unwrap() == krabka_units::bytes(3));
        assert!(
            parse("1KiB").unwrap() == krabka_units::bytes(1024),
            "units are honoured"
        );

        for rejected in ["1.5B", "-1B", "-0.5B", "18446744073709551616B"] {
            let err = parse(rejected).unwrap_err().to_string();
            assert!(
                err.contains("non-negative whole byte count"),
                "{rejected} should be rejected, got: {err}"
            );
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    #[test]
    fn require_service_name_injects_unknown() {
        let mut labels = labels(&[("__name__", "process_cpu")]);
        require_service_name(&mut labels);
        assert!(labels.get("service_name") == Some("unknown_service"));
    }

    #[test]
    fn require_service_name_keeps_existing() {
        let mut labels = labels(&[("__name__", "process_cpu"), ("service_name", "api")]);
        require_service_name(&mut labels);
        assert!(labels.get("service_name") == Some("api"));
    }

    #[test]
    fn session_id_is_modulo_hashed() {
        let mut a = labels(&[("__session_id__", "deadbeefcafef00d")]);
        cap_session_id(&mut a, 16);
        let value = a.get("__session_id__").unwrap();
        let bucket: u64 = value.parse().unwrap();
        assert!(bucket < 16);

        let mut b = labels(&[("__session_id__", "deadbeefcafef00d")]);
        cap_session_id(&mut b, 16);
        assert!(b.get("__session_id__") == a.get("__session_id__"));
    }

    #[test]
    fn enforce_limits_rejects_too_many_labels() {
        let limits = TenantLimits {
            max_label_names_per_series: 1,
            ..Default::default()
        };
        let labels = labels(&[("a", "1"), ("b", "2")]);
        assert!(enforce_limits(&labels, &limits).is_err());
    }

    #[test]
    fn enforce_limits_rejects_too_long_label_names() {
        let limits = TenantLimits {
            max_label_name: bytes(3),
            ..Default::default()
        };
        let labels = labels(&[("too_long", "1")]);
        assert!(enforce_limits(&labels, &limits).is_err());
    }

    #[test]
    fn tenant_limit_config_uses_override_before_default() {
        let config = TenantLimitConfig::default().with_tenant_limits(
            "tenant-a",
            TenantLimits {
                max_label_names_per_series: 2,
                max_label_value: bytes(5),
                session_id_buckets: 8,
                ..Default::default()
            },
        );

        assert!(config.for_tenant("tenant-a").max_label_value == bytes(5));
        assert!(config.for_tenant("tenant-b") == &TenantLimits::default());
    }

    #[test]
    fn tenant_limits_reject_invalid_label_byte_caps() {
        for cap in ["-1B", "1.5B", "18446744073709551616B"] {
            let json = serde_json::json!({
                "max_label_name": cap,
                "max_label_names_per_series": 30,
                "max_label_value": "2KiB",
                "session_id_buckets": 1024,
            });
            assert!(serde_json::from_value::<TenantLimits>(json).is_err());
        }
    }

    #[test]
    fn relabel_drop_rejects_series() {
        let mut labels = labels(&[("env", "dev"), ("__name__", "cpu")]);
        let config = RelabelConfig {
            source_labels: vec!["env".to_string()],
            regex: "dev".to_string(),
            target_label: String::new(),
            replacement: String::new(),
            action: RelabelAction::Drop,
        };
        assert!(!apply_relabel(&mut labels, &[config]));
    }
}

// === split-modules: generated submodules ===
mod apply_relabel;
mod cap_session_id;
mod decoded_profile;
mod decoded_sample;
mod default_max_label_name;
mod enforce_limits;
mod fnv1a;
mod raw_profile;
mod regex_anchored;
mod relabel_action;
mod relabel_config;
mod remove_label;
mod replace_label;
mod require_service_name;
mod tenant_limit_config;
mod tenant_limits;

pub use apply_relabel::apply_relabel;
pub use cap_session_id::cap_session_id;
pub use decoded_profile::DecodedProfile;
pub use decoded_sample::DecodedSample;
use default_max_label_name::default_max_label_name;
pub use enforce_limits::enforce_limits;
use fnv1a::fnv1a;
pub use raw_profile::RawProfile;
use regex_anchored::regex_anchored;
pub use relabel_action::RelabelAction;
pub use relabel_config::RelabelConfig;
use remove_label::remove_label;
use replace_label::replace_label;
pub use require_service_name::require_service_name;
pub use tenant_limit_config::TenantLimitConfig;
pub use tenant_limits::TenantLimits;
