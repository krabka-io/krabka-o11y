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

use crabka_blockstore::Labels;
use crabka_pprof::PprofProfile;
use crabka_units::{ByteSize, bytes, convert::ByteSizeExt as _};
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

/// One decoded pprof plus its series labels, before the multi-value split.
#[derive(Debug, Clone)]
pub struct RawProfile {
    pub labels: Labels,
    pub profile: PprofProfile,
    pub delta: bool,
    pub sample_timestamps_ns: Vec<Vec<i64>>,
    pub sample_span_ids: Vec<Option<u64>>,
    pub sample_trace_ids: Vec<Option<Vec<u8>>>,
}

/// One series after the multi-value split: a single `__profile_type__`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedProfile {
    pub labels: Labels,
    pub profile_type: String,
    pub samples: Vec<DecodedSample>,
}

/// One sample's raw payload, still unsymbolized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSample {
    pub stacktrace_location_refs: Vec<u32>,
    pub value: i64,
    pub timestamp_ns: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}

/// Per-tenant ingest limits for structural validation.
///
/// Not `Eq`: the label caps are [`ByteSize`] quantities, which store `f64`.
/// These limits are only ever a map value in `TenantLimitConfig::tenants`, so
/// nothing needs the derive.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantLimits {
    /// Cap on the UTF-8 bytes of a label name.
    #[serde(default = "default_max_label_name", with = "label_byte_limit")]
    pub max_label_name: ByteSize,
    pub max_label_names_per_series: usize,
    /// Cap on the UTF-8 bytes of a label value.
    #[serde(with = "label_byte_limit")]
    pub max_label_value: ByteSize,
    pub session_id_buckets: u64,
}

const fn default_max_label_name() -> ByteSize {
    bytes(1024)
}

mod label_byte_limit {
    use crabka_units::{ByteSize, convert::ByteSizeExt as _};
    use serde::{Deserializer, Serializer, de::Error as _};

    #[cfg(target_pointer_width = "64")]
    const USIZE_UPPER_EXCLUSIVE: f64 = 18_446_744_073_709_551_616.0;
    #[cfg(target_pointer_width = "32")]
    const USIZE_UPPER_EXCLUSIVE: f64 = 4_294_967_296.0;

    #[allow(clippy::trivially_copy_pass_by_ref)] // Required by serde's `with` adapter contract.
    pub fn serialize<S: Serializer>(value: &ByteSize, serializer: S) -> Result<S::Ok, S::Error> {
        crabka_units::serde_units::human::byte_size::serialize(value, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<ByteSize, D::Error> {
        let value = crabka_units::serde_units::human::byte_size::deserialize(deserializer)?;
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

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_label_name: default_max_label_name(),
            max_label_names_per_series: 30,
            max_label_value: bytes(2048),
            session_id_buckets: 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TenantLimitConfig {
    #[serde(default)]
    pub default: TenantLimits,
    #[serde(default)]
    pub tenants: BTreeMap<String, TenantLimits>,
}

impl TenantLimitConfig {
    #[must_use]
    pub fn with_tenant_limits(mut self, tenant: impl Into<String>, limits: TenantLimits) -> Self {
        self.tenants.insert(tenant.into(), limits);
        self
    }

    #[must_use]
    pub fn for_tenant(&self, tenant: &str) -> &TenantLimits {
        self.tenants.get(tenant).unwrap_or(&self.default)
    }
}

/// A Prometheus-style relabel rule subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelabelConfig {
    pub source_labels: Vec<String>,
    pub regex: String,
    pub target_label: String,
    pub replacement: String,
    pub action: RelabelAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelabelAction {
    Replace,
    Keep,
    Drop,
}

/// Inject `service_name="unknown_service"` when absent or empty.
pub fn require_service_name(labels: &mut Labels) {
    if labels.get("service_name").unwrap_or("").is_empty() {
        labels.insert("service_name", "unknown_service");
    }
}

/// Cap the cardinality of `__session_id__` with a stable modulo hash.
pub fn cap_session_id(labels: &mut Labels, buckets: u64) {
    let Some(raw) = labels.get("__session_id__").map(str::to_owned) else {
        return;
    };
    let bucket = fnv1a(raw.as_bytes()) % buckets.max(1);
    replace_label(labels, "__session_id__", &bucket.to_string());
}

fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// Enforce per-tenant structural caps.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn enforce_limits(labels: &Labels, limits: &TenantLimits) -> Result<(), ProfilesError> {
    if labels.len() > limits.max_label_names_per_series {
        return Err(ProfilesError::Invalid(format!(
            "too many label names: {} > {}",
            labels.len(),
            limits.max_label_names_per_series
        )));
    }

    for (name, value) in labels.iter() {
        if name.len() > limits.max_label_name.bytes_usize() {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` name exceeds {} bytes",
                limits.max_label_name.bytes_usize()
            )));
        }
        if value.len() > limits.max_label_value.bytes_usize() {
            return Err(ProfilesError::Invalid(format!(
                "label `{name}` value exceeds {} bytes",
                limits.max_label_value.bytes_usize()
            )));
        }
    }

    Ok(())
}

/// Apply relabel rules in order. Returns `false` when a rule rejects the series.
pub fn apply_relabel(labels: &mut Labels, configs: &[RelabelConfig]) -> bool {
    for config in configs {
        let joined = config
            .source_labels
            .iter()
            .map(|name| labels.get(name).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(";");
        let Ok(regex) = regex_anchored(&config.regex) else {
            continue;
        };
        let matched = regex.is_match(&joined);

        match config.action {
            RelabelAction::Drop if matched => return false,
            RelabelAction::Keep if !matched => return false,
            RelabelAction::Replace if matched => {
                if config.replacement.is_empty() {
                    remove_label(labels, &config.target_label);
                } else {
                    replace_label(labels, &config.target_label, &config.replacement);
                }
            }
            RelabelAction::Drop | RelabelAction::Keep | RelabelAction::Replace => {}
        }
    }
    true
}

fn regex_anchored(pattern: &str) -> Result<regex::Regex, regex::Error> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
}

fn replace_label(labels: &mut Labels, target: &str, replacement: &str) {
    let mut rebuilt = Labels::new();
    for (name, value) in labels.iter() {
        if name != target {
            rebuilt.insert(name, value);
        }
    }
    rebuilt.insert(target, replacement);
    *labels = rebuilt;
}

fn remove_label(labels: &mut Labels, target: &str) {
    let mut rebuilt = Labels::new();
    for (name, value) in labels.iter() {
        if name != target {
            rebuilt.insert(name, value);
        }
    }
    *labels = rebuilt;
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use crabka_blockstore::Labels;

    use super::*;

    /// FNV-1a, checked against the reference vectors rather than against
    /// values this implementation produced. The constants and the
    /// xor-then-multiply order are the whole algorithm, and a version that
    /// multiplies before xoring, or seeds from zero, still looks like a hash.
    #[test]
    fn fnv1a_matches_the_published_vectors() {
        let hash = |s: &str| super::fnv1a(s.as_bytes());

        assert!(hash("") == 0xcbf2_9ce4_8422_2325, "the empty input is the offset basis");
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

    fn labels_of(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert(*name, *value);
        }
        labels
    }

    /// `apply_relabel` returns whether the series survives. Drop and Keep are
    /// mirror images -- one rejects on a match, the other on the absence of
    /// one -- so both are checked from both sides.
    #[test]
    fn relabel_drop_and_keep_are_mirror_images() {
        let mut labels = labels_of(&[("env", "prod")]);

        assert!(!apply_relabel(&mut labels, &[relabel(RelabelAction::Drop, &["env"], "prod")]));
        assert!(apply_relabel(&mut labels, &[relabel(RelabelAction::Drop, &["env"], "dev")]));
        assert!(apply_relabel(&mut labels, &[relabel(RelabelAction::Keep, &["env"], "prod")]));
        assert!(!apply_relabel(&mut labels, &[relabel(RelabelAction::Keep, &["env"], "dev")]));

        // The regex is anchored, so a partial match is not a match.
        assert!(apply_relabel(&mut labels, &[relabel(RelabelAction::Drop, &["env"], "pro")]));

        // A label that is not set reads as empty rather than skipping the rule.
        assert!(!apply_relabel(&mut labels, &[relabel(RelabelAction::Keep, &["absent"], "prod")]));
        assert!(apply_relabel(&mut labels, &[relabel(RelabelAction::Keep, &["absent"], "")]));

        // Several source labels are joined with ';' before matching.
        let mut two = labels_of(&[("a", "x"), ("b", "y")]);
        assert!(!apply_relabel(&mut two, &[relabel(RelabelAction::Drop, &["a", "b"], "x;y")]));
        assert!(apply_relabel(&mut two, &[relabel(RelabelAction::Drop, &["a", "b"], "xy")]));

        // A rule whose regex will not compile is skipped, not treated as a
        // match: one bad rule must not drop every series.
        assert!(apply_relabel(&mut labels, &[relabel(RelabelAction::Keep, &["env"], "[")]));
    }

    /// A Replace rule with an empty replacement removes the target label
    /// instead of setting it to the empty string, and touches nothing else.
    #[test]
    fn relabel_replace_sets_or_removes_only_the_target() {
        let mut labels = labels_of(&[("env", "prod"), ("target", "old"), ("keep", "me")]);
        let mut config = relabel(RelabelAction::Replace, &["env"], "prod");

        assert!(apply_relabel(&mut labels, std::slice::from_ref(&config)));
        assert!(labels == labels_of(&[("env", "prod"), ("target", "new"), ("keep", "me")]));

        config.replacement = String::new();
        assert!(apply_relabel(&mut labels, std::slice::from_ref(&config)));
        assert!(labels == labels_of(&[("env", "prod"), ("keep", "me")]));

        // A rule that does not match leaves the labels alone.
        config.replacement = "other".to_string();
        config.regex = "dev".to_string();
        assert!(apply_relabel(&mut labels, std::slice::from_ref(&config)));
        assert!(labels == labels_of(&[("env", "prod"), ("keep", "me")]));
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
