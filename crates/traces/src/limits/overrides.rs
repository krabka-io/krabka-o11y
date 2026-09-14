use std::{collections::HashMap, sync::RwLock};

use krabka_blockstore::RetentionWindows;
use krabka_units::{
    ByteSize, Frequency, Time,
    convert::{ByteSizeExt as _, FrequencyExt as _, TimeExt},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use super::Limits;

#[cfg(test)]
mod tests {
    use krabka_units::{bytes, hours, per_sec};

    use super::*;
    use crate::limits::Limits;

    const YAML: &str = r"
overrides:
  tenant-a:
    ingestion_rate_spans_per_sec: 500
    max_spans_per_trace: 1000
  tenant-b:
    max_attribute_bytes: 64
";

    /// The retention window is per tenant, and the sweep reads it through
    /// [`RetentionWindows`] rather than through [`OverridesProvider`], so the
    /// trait is what this checks.
    #[test]
    fn each_tenant_answers_the_sweep_with_its_own_retention_window() {
        let provider = OverridesProvider::from_yaml(
            r"
overrides:
  short:
    block_retention: 48h
  forever:
    block_retention: 0s
",
        )
        .unwrap();

        assert2::check!(provider.block_retention("short") == hours(48));
        assert2::check!(provider.block_retention("forever") == <Time as TimeExt>::ZERO);
        assert2::check!(
            provider.block_retention("unlisted") == Limits::default().block_retention,
            "an unlisted tenant reads the defaults"
        );
        assert2::check!(provider.expires_any_blocks());
    }

    /// A deployment where nothing can ever expire should not pay for a sweep.
    /// The default window counts, because it is what every unlisted tenant
    /// reads.
    #[test]
    fn a_deployment_with_no_window_anywhere_expires_nothing() {
        let defaults = Limits {
            block_retention: <Time as TimeExt>::ZERO,
            ..Limits::default()
        };
        let provider = OverridesProvider::from_yaml_with_defaults(
            r"
overrides:
  tenant-a:
    max_spans_per_trace: 1000
",
            defaults,
        )
        .unwrap();

        assert2::check!(!provider.expires_any_blocks());
        assert2::check!(OverridesProvider::new(Limits::default()).expires_any_blocks());
    }

    #[test]
    fn tenant_override_merges_over_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_a = provider.for_tenant("tenant-a");

        // Overridden fields take the yaml values; the rest keep the defaults.
        assert2::assert!(
            tenant_a
                == Limits {
                    ingestion_rate: per_sec(500),
                    ingestion_burst_spans: 100_000,
                    max_spans_per_request: 10_000,
                    max_traces_per_search: 1000,
                    max_spans_per_trace: 1000,
                    max_attribute: bytes(2048),
                    max_search_duration: <Time as TimeExt>::ZERO,
                    block_retention: Limits::default().block_retention,
                }
        );
    }

    #[test]
    fn partial_override_keeps_other_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_b = provider.for_tenant("tenant-b");

        assert2::assert!(tenant_b.max_attribute == bytes(64));
        assert2::assert!(tenant_b.ingestion_rate == Limits::default().ingestion_rate);
    }

    /// The defaults a service was started with, not the compiled ones, are what
    /// an unlisted tenant gets and what a listed tenant's entry merges over.
    /// Without that, a command-line flag would reach only the tenants no file
    /// mentions, and the file would silently undo it for the rest.
    #[test]
    fn process_defaults_carry_into_every_tenant() {
        let defaults = Limits {
            max_spans_per_trace: 7,
            max_traces_per_search: 9,
            ..Limits::default()
        };
        let provider = OverridesProvider::from_yaml_with_defaults(YAML, defaults).unwrap();

        // The entry names `max_spans_per_trace`, so that key wins for tenant-a.
        assert2::assert!(provider.for_tenant("tenant-a").max_spans_per_trace == 1000);
        // It names nothing else, so the process defaults stand.
        assert2::assert!(provider.for_tenant("tenant-a").max_traces_per_search == 9);
        // tenant-b's entry names neither, so both process defaults stand.
        assert2::assert!(
            provider.for_tenant("tenant-b")
                == Limits {
                    max_attribute: bytes(64),
                    ..defaults
                }
        );
        assert2::assert!(provider.for_tenant("tenant-z") == defaults);
    }

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();

        assert2::assert!(provider.for_tenant("tenant-z") == Limits::default());
    }

    #[test]
    fn api_overrides_use_versions_and_merge_patches() {
        let provider = OverridesProvider::new(Limits::default());

        assert2::assert!(
            provider.api_set(
                "tenant-a",
                serde_json::json!({"max_spans_per_trace": 7}),
                "0",
            ) == Ok("1".into())
        );
        assert2::assert!(
            provider.api_set("tenant-a", serde_json::json!({}), "0")
                == Err(OverrideMutationError::VersionMismatch)
        );
        assert2::assert!(
            provider
                .api_patch(
                    "tenant-a",
                    serde_json::json!({"max_spans_per_trace": null, "max_traces_per_search": 9}),
                )
                .map(|(_, version)| version)
                == Ok("2".into())
        );
        assert2::assert!(
            provider.for_tenant("tenant-a").max_spans_per_trace
                == Limits::default().max_spans_per_trace
        );
        assert2::assert!(provider.for_tenant("tenant-a").max_traces_per_search == 9);
        assert2::assert!(
            provider.api_delete("tenant-a", "1") == Err(OverrideMutationError::VersionMismatch)
        );
        assert2::assert!(provider.api_delete("tenant-a", "2").is_ok());
        assert2::assert!(provider.api_get("tenant-a").is_none());
    }

    #[test]
    fn api_overrides_are_shared_and_survive_provider_restart() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("trace-api-overrides.json");
        let first = OverridesProvider::new(Limits::default())
            .with_api_file(path.clone())
            .unwrap();
        let second = OverridesProvider::new(Limits::default())
            .with_api_file(path.clone())
            .unwrap();

        assert2::assert!(
            first.api_set(
                "tenant-a",
                serde_json::json!({"max_spans_per_trace": 7}),
                "0",
            ) == Ok("1".into())
        );
        assert2::check!(second.for_tenant("tenant-a").max_spans_per_trace == 7);

        drop(first);
        drop(second);
        let restarted = OverridesProvider::new(Limits::default())
            .with_api_file(path)
            .unwrap();
        assert2::check!(restarted.for_tenant("tenant-a").max_spans_per_trace == 7);
        assert2::check!(restarted.api_get("tenant-a").unwrap().1 == "1");
    }

    #[test]
    fn trace_limits_accept_metrics_generator_settings_in_the_shared_file() {
        let provider = OverridesProvider::from_yaml(
            r"
overrides:
  tenant-a:
    max_spans_per_trace: 7
    metrics_generator:
      processor:
        host_info:
          enabled: true
",
        )
        .unwrap();

        assert2::assert!(provider.for_tenant("tenant-a").max_spans_per_trace == 7);
    }
}

mod merge_limits;
mod overrides_error;
mod overrides_provider;
mod partial_limits;
mod runtime_file;

use merge_limits::merge_limits;
pub use overrides_error::OverridesError;
pub use overrides_provider::{OverrideMutationError, OverridesProvider};
use partial_limits::PartialLimits;
use runtime_file::RuntimeFile;
