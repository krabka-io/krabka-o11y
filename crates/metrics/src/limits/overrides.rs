use std::collections::HashMap;

use krabka_blockstore::RetentionWindows;
use krabka_units::{prelude::*, serde_units};
use serde::Deserialize;
use thiserror::Error;

use super::Limits;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    const YAML: &str = r#"
overrides:
  tenant-a:
    ingestion_rate: "500/s"
    max_global_series_per_user: 1000
  tenant-b:
    max_label_value_length: "64B"
  tenant-c:
    out_of_order_time_window: "1500ms"
    max_query_length: "1h"
    max_query_lookback: "7d"
  tenant-d:
    max_series_per_request: 3
    max_samples_per_series: 5
    creation_grace_period: "45s"
    active_series_idle_timeout: "2m"
    otlp_delta_max_stale: "90s"
    otlp_delta_max_streams: 7
  tenant-e:
    compactor_blocks_retention_period: "72h"
"#;

    /// The request-shape limits reach the same per-tenant override path as
    /// every other limit. Both are counts with neighbouring names, so the two
    /// values differ: a field that reads its neighbour still parses, and only
    /// distinct numbers show it.
    #[test]
    fn request_shape_limits_are_settable_per_tenant() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let d = p.for_tenant("tenant-d");

        check!(d.max_series_per_request == 3);
        check!(d.max_samples_per_series == 5, "not the series cap");
        check!(d.creation_grace_period == secs(45));
        check!(
            p.for_tenant("tenant-a").max_series_per_request
                == Limits::default().max_series_per_request,
            "an unlisted field keeps the default"
        );
        check!(
            p.for_tenant("tenant-a").max_samples_per_series
                == Limits::default().max_samples_per_series
        );
    }

    /// The two eviction windows and the OTLP stream cap are per-tenant too,
    /// because a tenant with long-lived series and one with churn need
    /// different windows. Each value here is distinct from the others and
    /// from every default.
    #[test]
    fn eviction_windows_are_settable_per_tenant() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let d = p.for_tenant("tenant-d");

        check!(d.active_series_idle_timeout == minutes(2));
        check!(d.otlp_delta_max_stale == secs(90), "not the idle timeout");
        check!(d.otlp_delta_max_streams == 7);

        let a = p.for_tenant("tenant-a");
        check!(
            a.active_series_idle_timeout == minutes(20),
            "Mimir's default"
        );
        check!(a.otlp_delta_max_stale == minutes(5));
    }

    #[test]
    fn tenant_override_merges_over_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let a = p.for_tenant("tenant-a");
        check!(a.ingestion_rate == per_sec(500));
        check!(a.max_global_series_per_user == 1000);
        check!(a.max_label_name_length == Limits::default().max_label_name_length);
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let b = p.for_tenant("tenant-b");
        assert!(b.max_label_value_length == bytes(64));
        assert!(b.ingestion_rate == Limits::default().ingestion_rate);
    }

    #[test]
    fn parses_out_of_order_window_override() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert!(p.for_tenant("tenant-c").out_of_order_time_window == millis(1500));
        assert!(p.for_tenant("tenant-a").out_of_order_time_window == Time::ZERO);
    }

    #[test]
    fn parses_query_span_cap_overrides() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        let c = p.for_tenant("tenant-c");
        check!(c.max_query_length == hours(1));
        check!(c.max_query_lookback == days(7));
        check!(p.for_tenant("tenant-a").max_query_length == Time::ZERO);
    }

    /// The retention window is per tenant, and it is the window the sweep
    /// reads through `RetentionWindows`. A tenant with no entry of its own
    /// answers from the defaults, and the built-in default is zero, which
    /// keeps every block forever. A sweep that read zero as "delete
    /// everything" would empty the bucket of every unconfigured tenant, so
    /// the zero case is pinned through the trait the sweep calls.
    #[test]
    fn block_retention_is_per_tenant_and_zero_by_default() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();

        check!(p.for_tenant("tenant-e").compactor_blocks_retention_period == hours(72));
        check!(p.block_retention("tenant-e") == hours(72));
        check!(
            p.block_retention("tenant-a") == Time::ZERO,
            "a listed tenant without the key keeps the default"
        );
        check!(
            p.block_retention("tenant-z") == Time::ZERO,
            "an unlisted tenant keeps the default"
        );
        check!(Limits::default().compactor_blocks_retention_period == Time::ZERO);
    }

    /// The defaults block moves the window of every tenant that does not set
    /// one, including the tenants the file never names. The sweep lists the
    /// whole bucket for exactly this reason.
    #[test]
    fn a_default_block_retention_window_reaches_unlisted_tenants() {
        let p = OverridesProvider::from_yaml(
            "defaults:\n  compactor_blocks_retention_period: \"24h\"\noverrides:\n  tenant-a:\n    compactor_blocks_retention_period: \"1h\"\n",
        )
        .unwrap();

        check!(p.block_retention("tenant-a") == hours(1));
        check!(p.block_retention("tenant-z") == hours(24));
    }

    /// A sweep costs a pass over the whole bucket, so a deployment where no
    /// tenant can ever expire a block does not run one. The question is asked
    /// of the defaults as well as of every named tenant: a default window
    /// with no tenant entry at all still expires blocks.
    #[test]
    fn a_deployment_with_no_window_anywhere_expires_nothing() {
        let cases: &[(&str, bool)] = &[
            (
                "overrides:\n  tenant-a:\n    max_series_per_request: 3\n",
                false,
            ),
            (
                "overrides:\n  tenant-a:\n    compactor_blocks_retention_period: \"0s\"\n",
                false,
            ),
            (
                "overrides:\n  tenant-a:\n    compactor_blocks_retention_period: \"1ms\"\n",
                true,
            ),
            (
                "defaults:\n  compactor_blocks_retention_period: \"1h\"\n",
                true,
            ),
        ];

        for (yaml, expected) in cases {
            let p = OverridesProvider::from_yaml(yaml).unwrap();
            check!(p.expires_any_blocks() == *expected, "{yaml}");
        }
    }

    /// A misspelled key is refused, not ignored. A dropped
    /// `compactor_blocks_retention_period` would leave the operator believing
    /// a window was set while the bucket grew without bound, and nothing
    /// would report it. Mimir 2.16.1 refuses the same key the same way.
    #[test]
    fn a_misspelled_override_key_is_refused() {
        for yaml in [
            "overrides:\n  tenant-a:\n    compactor_block_retention_period: \"24h\"\n",
            "overrides:\n  tenant-a:\n    max_series_per_requests: 3\n",
            "defaults:\n  compactor_block_retention_period: \"24h\"\n",
        ] {
            let error = OverridesProvider::from_yaml(yaml).unwrap_err();
            let OverridesError::Yaml(message) = &error;
            check!(
                message.contains("unknown field"),
                "{yaml} was accepted with: {message}"
            );
        }

        check!(
            OverridesProvider::from_yaml(
                "overrides:\n  tenant-a:\n    compactor_blocks_retention_period: \"24h\"\n"
            )
            .is_ok(),
            "the spelling Mimir uses is accepted"
        );
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let p = OverridesProvider::from_yaml(YAML).unwrap();
        assert!(*p.for_tenant("tenant-z") == Limits::default());
    }

    #[test]
    fn dimensioned_override_without_a_unit_is_rejected() {
        // A bare `30` for a window that used to be `_ms` must not be guessed at;
        // the human encoding demands the unit the type now carries.
        let error = OverridesProvider::from_yaml(
            "overrides:\n  tenant-a:\n    out_of_order_time_window: 1500\n",
        )
        .unwrap_err();

        assert!(matches!(error, OverridesError::Yaml(_)));
    }

    /// A negative cap would read as "unlimited" downstream, because the
    /// enforcer applies only a cap greater than zero. Zero is the documented
    /// sentinel.
    #[test]
    fn negative_query_span_caps_are_rejected() {
        const NEGATIVE: &str = "overrides:\n  tenant-a:\n    max_query_length: \"-1s\"\n";
        const ZERO: &str = "overrides:\n  tenant-a:\n    max_query_length: \"0\"\n";

        assert!(let Err(_) = OverridesProvider::from_yaml(NEGATIVE));
        assert!(let Ok(_) = OverridesProvider::from_yaml(ZERO));
    }
}

mod merge_limits;
mod overrides_error;
mod overrides_provider;
mod partial_limits;
mod runtime_file;

use merge_limits::merge_limits;
pub use overrides_error::OverridesError;
pub use overrides_provider::OverridesProvider;
use partial_limits::PartialLimits;
use runtime_file::RuntimeFile;
