use std::collections::HashMap;

use krabka_blockstore::TenantId;
use krabka_units::{
    ByteSize, Frequency, Time,
    convert::{ByteSizeExt as _, FrequencyExt, TimeExt as _},
};
use serde::Deserialize;

use super::Limits;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{bytes, per_sec, secs};

    use super::*;

    const YAML: &str = r"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 500
    max_series: 1000
  tenant-b:
    max_label_value_length: 64
";

    #[test]
    fn tenant_override_merges_over_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_a = provider.for_tenant(&"tenant-a".parse().unwrap());

        assert!(
            *tenant_a
                == Limits {
                    ingestion_rate: per_sec(500),
                    ingestion_burst_profiles: 10_000,
                    max_series: 1000,
                    max_label_name: bytes(1024),
                    max_label_value: bytes(2048),
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 2048,
                    max_flamegraph_nodes_max: 0,
                    max_query_length: secs(2_595_600),
                    max_session_id_cardinality: 0,
                }
        );
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_b = provider.for_tenant(&"tenant-b".parse().unwrap());

        assert!(tenant_b.max_label_value == bytes(64));
        assert!(tenant_b.ingestion_rate == Limits::default().ingestion_rate);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();

        check!(*provider.for_tenant(&"tenant-z".parse().unwrap()) == Limits::default());
    }

    /// The `defaults` block moves every tenant that has no entry of its own,
    /// and a tenant entry still departs from it rather than from the
    /// compiled-in value.
    #[test]
    fn the_defaults_block_moves_every_tenant() {
        let provider = OverridesProvider::from_yaml(
            r"
defaults:
  max_label_value_length: 64
  max_label_names_per_series: 5
  max_session_id_cardinality: 8
overrides:
  tenant-a:
    max_label_names_per_series: 7
",
        )
        .unwrap();

        assert!(
            *provider.for_tenant(&"unlisted".parse().unwrap())
                == Limits {
                    max_label_value: bytes(64),
                    max_label_names_per_series: 5,
                    max_session_id_cardinality: 8,
                    ..Limits::default()
                }
        );
        assert!(
            *provider.for_tenant(&"tenant-a".parse().unwrap())
                == Limits {
                    max_label_value: bytes(64),
                    max_label_names_per_series: 7,
                    max_session_id_cardinality: 8,
                    ..Limits::default()
                }
        );
    }

    /// A byte cap is a whole non-negative count of bytes. Serde rejects each
    /// way a YAML scalar can fail to be one, and rejecting is the point: a cap
    /// that silently rounded or wrapped would be enforced as something other
    /// than what was configured.
    #[test]
    fn a_byte_cap_must_be_a_whole_non_negative_count() {
        let parse = |key: &str, value: &str| {
            OverridesProvider::from_yaml(&format!("overrides:\n  tenant-a:\n    {key}: {value}\n"))
        };
        let tenant_a = |provider: &OverridesProvider| {
            provider.for_tenant(&"tenant-a".parse().unwrap()).clone()
        };

        check!(
            tenant_a(&parse("max_label_name_length", "0").unwrap()).max_label_name == bytes(0),
            "zero is a cap, and means unlimited"
        );
        check!(
            tenant_a(&parse("max_label_value_length", "1024").unwrap()).max_label_value
                == bytes(1024)
        );

        for key in ["max_label_name_length", "max_label_value_length"] {
            for rejected in ["-1", "1.5", "-0.5", "18446744073709551616"] {
                let err = parse(key, rejected).unwrap_err();
                check!(
                    matches!(err, OverridesError::Yaml(_)),
                    "{key}: {rejected} should be rejected, got: {err:?}"
                );
            }
        }
    }

    #[test]
    fn an_invalid_defaults_block_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r"
defaults:
  max_flamegraph_nodes_max: -5
",
        )
        .unwrap_err();

        assert!(
            matches!(err, OverridesError::InvalidDefaults { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn unknown_tenant_key_is_rejected() {
        // `max_serie` is a typo of `max_series`; with `deny_unknown_fields`
        // this is now a load error rather than a silently-ignored field.
        let err = OverridesProvider::from_yaml(
            r"
overrides:
  tenant-a:
    max_serie: 1000
",
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Yaml(_)), "{err:?}");
    }

    #[test]
    fn unknown_top_level_key_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r"
overrides: {}
bogus_top_level: true
",
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Yaml(_)), "{err:?}");
    }

    #[test]
    fn negative_ingestion_rate_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: -1
",
        )
        .unwrap_err();

        assert!(
            matches!(err, OverridesError::Invalid { ref tenant, .. } if tenant == "tenant-a"),
            "{err:?}"
        );
    }

    #[test]
    fn non_finite_ingestion_rate_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: .nan
",
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Invalid { .. }), "{err:?}");
    }

    #[test]
    fn negative_flamegraph_node_cap_is_rejected() {
        let err = OverridesProvider::from_yaml(
            r"
overrides:
  tenant-a:
    max_flamegraph_nodes_max: -5
",
        )
        .unwrap_err();

        assert!(matches!(err, OverridesError::Invalid { .. }), "{err:?}");
    }

    #[test]
    fn zero_and_positive_numeric_values_are_accepted() {
        let provider = OverridesProvider::from_yaml(
            r"
overrides:
  tenant-a:
    ingestion_rate_profiles_per_sec: 0
    max_flamegraph_nodes_default: 0
    max_flamegraph_nodes_max: 4096
",
        )
        .unwrap();
        let tenant_a = provider.for_tenant(&"tenant-a".parse().unwrap());

        assert!(
            *tenant_a
                == Limits {
                    ingestion_rate: <Frequency as FrequencyExt>::ZERO,
                    ingestion_burst_profiles: 10_000,
                    max_series: 0,
                    max_label_name: bytes(1024),
                    max_label_value: bytes(2048),
                    max_label_names_per_series: 40,
                    max_flamegraph_nodes_default: 0,
                    max_flamegraph_nodes_max: 4096,
                    max_query_length: secs(2_595_600),
                    max_session_id_cardinality: 0,
                }
        );
    }
}

mod overrides_error;
mod overrides_provider;
mod partial_limits;
mod runtime_file;

pub use overrides_error::OverridesError;
pub use overrides_provider::OverridesProvider;
use partial_limits::PartialLimits;
use runtime_file::RuntimeFile;
