use std::{collections::HashMap, sync::RwLock};

use krabka_blockstore::RetentionWindows;
use krabka_query_frontend::AdmissionLimitsOverride;
use krabka_units::{
    ByteSize, Frequency, Time,
    convert::{ByteSizeExt as _, FrequencyExt as _, TimeExt},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

use super::Limits;

pub(crate) fn merge_limits(defaults: &Limits, partial: &PartialLimits) -> Limits {
    Limits {
        query_admission: defaults.query_admission.merge(partial.query_admission),
        ingestion_rate: partial
            .ingestion_rate_spans_per_sec
            .map_or(defaults.ingestion_rate, Frequency::from_per_sec),
        ingestion_burst_spans: partial
            .ingestion_burst_spans
            .unwrap_or(defaults.ingestion_burst_spans),
        max_spans_per_request: partial
            .max_spans_per_request
            .unwrap_or(defaults.max_spans_per_request),
        max_traces_per_search: partial
            .max_traces_per_search
            .unwrap_or(defaults.max_traces_per_search),
        max_spans_per_trace: partial
            .max_spans_per_trace
            .unwrap_or(defaults.max_spans_per_trace),
        max_attribute: partial
            .max_attribute_bytes
            .map_or(defaults.max_attribute, ByteSize::from_bytes),
        max_search_duration: partial
            .max_search_duration_secs
            .map_or(defaults.max_search_duration, |secs| {
                Time::from_secs(i64::try_from(secs).unwrap_or(i64::MAX))
            }),
        block_retention: partial.block_retention.unwrap_or(defaults.block_retention),
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OverridesError {
    #[error("failed to parse overrides yaml: {0}")]
    Yaml(String),
}

// The Tempo-shaped runtime-overrides keys, in the units an operator writes them
// (spans/sec, bytes, seconds). This is intentionally partial configuration, not
// old-schema compatibility: each tenant entry overrides only the limit fields it
// names, and `merge_limits` lifts them into the dimensioned `Limits`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct PartialLimits {
    pub(crate) query_admission: AdmissionLimitsOverride,
    pub(crate) ingestion_rate_spans_per_sec: Option<f64>,
    pub(crate) ingestion_burst_spans: Option<u64>,
    pub(crate) max_spans_per_request: Option<u64>,
    pub(crate) max_traces_per_search: Option<u64>,
    pub(crate) max_spans_per_trace: Option<u64>,
    pub(crate) max_attribute_bytes: Option<u64>,
    pub(crate) max_search_duration_secs: Option<u64>,
    // Tempo writes this one as a duration rather than a count, and it is the
    // only knob here an operator copies straight out of a Tempo overrides
    // file, so it keeps Tempo's spelling and Tempo's value form: `336h`.
    #[serde(with = "krabka_units::serde_units::human::option_time")]
    pub(crate) block_retention: Option<Time>,
    // Parsed by `MetricsGenConfig` from the same document. Keeping it here
    // lets the trace-limit reader validate a real Tempo override entry without
    // owning the metrics-generator configuration.
    pub(crate) metrics_generator: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub(crate) struct RuntimeFile {
    #[serde(default)]
    pub(crate) overrides: HashMap<String, PartialLimits>,
}

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
                    query_admission: krabka_query_frontend::AdmissionLimits::default(),
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
    fn query_admission_is_resolved_per_tenant() {
        let provider = OverridesProvider::from_yaml(
            "overrides:\n  tenant-a:\n    query_admission:\n      max_concurrent_subqueries_per_tenant: 7\n",
        )
        .unwrap();

        assert2::check!(
            provider
                .for_tenant("tenant-a")
                .query_admission
                .max_concurrent_subqueries_per_tenant
                == 7
        );
        assert2::check!(
            provider.for_tenant("tenant-b").query_admission
                == krabka_query_frontend::AdmissionLimits::default()
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
                .map(|snapshot| snapshot.version)
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
        assert2::check!(restarted.api_get("tenant-a").unwrap().version == "1");
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

mod overrides_provider;

pub use overrides_provider::{OverrideMutationError, OverridesProvider};
