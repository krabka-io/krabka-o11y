use std::collections::HashMap;

use krabka_units::{
    ByteSize, Frequency, Time,
    convert::{ByteSizeExt as _, FrequencyExt as _, TimeExt},
};
use serde::Deserialize;
use thiserror::Error;

use super::Limits;

#[cfg(test)]
mod tests {
    use krabka_units::{bytes, per_sec};

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

    #[test]
    fn tenant_override_merges_over_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();
        let tenant_a = provider.for_tenant("tenant-a");

        // Overridden fields take the yaml values; the rest keep the defaults.
        assert2::assert!(
            *tenant_a
                == Limits {
                    ingestion_rate: per_sec(500),
                    ingestion_burst_spans: 100_000,
                    max_traces_per_search: 1000,
                    max_spans_per_trace: 1000,
                    max_attribute: bytes(2048),
                    max_search_duration: <Time as TimeExt>::ZERO,
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

    #[test]
    fn unlisted_tenant_gets_defaults() {
        let provider = OverridesProvider::from_yaml(YAML).unwrap();

        assert2::assert!(*provider.for_tenant("tenant-z") == Limits::default());
    }
}

// === split-modules: generated submodules ===
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
