//! Prometheus-shaped query result model.

use krabka_blockstore::Labels;
use krabka_metrics::NativeHistogram;

#[cfg(test)]
mod tests {

    use krabka_blockstore::Labels;

    use super::*;

    #[test]
    fn result_type_strings_match_prometheus() {
        for (result, want) in [
            (
                QueryResult::Scalar {
                    ts_ms: 0,
                    value: 1.0,
                },
                "scalar",
            ),
            (QueryResult::InstantVector(vec![]), "vector"),
            (QueryResult::RangeMatrix(vec![]), "matrix"),
            (
                QueryResult::Str {
                    ts_ms: 0,
                    value: "x".into(),
                },
                "string",
            ),
        ] {
            assert2::assert!(result.result_type() == want);
        }
    }

    #[test]
    fn instant_sample_holds_float() {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        let sample = InstantSample {
            labels,
            ts_ms: 1000,
            value: SampleValue::Float(1.0),
        };
        assert2::assert!(sample.value == SampleValue::Float(1.0));
    }

    #[test]
    fn annotations_extend_merges_and_deduplicates() {
        let mut annotations = Annotations::new();
        annotations.warn("mixed float and histogram samples");
        annotations.info("histogram ignored");

        let mut other = Annotations::new();
        other.warn("mixed float and histogram samples");
        other.warn("counter reset detected");
        other.info("histogram ignored");
        other.info("stale sample skipped");

        annotations.extend(&other);

        assert2::assert!(
            annotations
                == Annotations {
                    warnings: vec![
                        "mixed float and histogram samples".to_string(),
                        "counter reset detected".to_string(),
                    ],
                    infos: vec![
                        "histogram ignored".to_string(),
                        "stale sample skipped".to_string(),
                    ],
                }
        );
    }

    #[test]
    fn annotations_empty_requires_no_warning_or_info_messages() {
        let mut warnings = Annotations::new();
        warnings.warn("warn");
        let mut infos = Annotations::new();
        infos.info("info");

        for (_case, annotations, want) in [
            ("new", Annotations::new(), true),
            ("warn", warnings, false),
            ("info", infos, false),
        ] {
            assert2::assert!(annotations.is_empty() == want);
        }
    }
}

mod annotations;
mod instant_sample;
mod query_result;
mod range_series;
mod sample_value;

pub use annotations::Annotations;
pub use instant_sample::InstantSample;
pub use query_result::QueryResult;
pub use range_series::RangeSeries;
pub use sample_value::SampleValue;
