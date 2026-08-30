use std::collections::HashMap;

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
"#;

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
