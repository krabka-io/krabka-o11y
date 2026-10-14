//! Prometheus-shaped query result model.

use krabka_metrics::NativeHistogram;

use crate::PromqlLabels as Labels;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::PromqlLabels as Labels;

    struct ResultTypeCase {
        result: QueryResult,
        expected: &'static str,
    }

    struct EmptyAnnotationCase {
        annotations: Annotations,
        expected_empty: bool,
    }

    #[test]
    fn result_type_strings_match_prometheus() {
        for case in [
            ResultTypeCase {
                result: QueryResult::Scalar {
                    ts_ms: 0,
                    value: 1.0,
                },
                expected: "scalar",
            },
            ResultTypeCase {
                result: QueryResult::InstantVector(vec![]),
                expected: "vector",
            },
            ResultTypeCase {
                result: QueryResult::RangeMatrix(vec![]),
                expected: "matrix",
            },
            ResultTypeCase {
                result: QueryResult::Str {
                    ts_ms: 0,
                    value: "x".into(),
                },
                expected: "string",
            },
        ] {
            assert2::assert!(case.result.result_type() == case.expected);
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
            drop_name: false,
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
                    ..crate::Annotations::default()
                }
        );
    }

    #[test]
    fn annotations_empty_requires_no_warning_or_info_messages() {
        let mut warnings = Annotations::new();
        warnings.warn("warn");
        let mut infos = Annotations::new();
        infos.info("info");

        for case in [
            EmptyAnnotationCase {
                annotations: Annotations::new(),
                expected_empty: true,
            },
            EmptyAnnotationCase {
                annotations: warnings,
                expected_empty: false,
            },
            EmptyAnnotationCase {
                annotations: infos,
                expected_empty: false,
            },
        ] {
            assert2::assert!(case.annotations.is_empty() == case.expected_empty);
        }
    }
}

mod annotated_query_result;
mod annotations;
mod float_sample;
mod histogram_quantile_repair;
mod instant_sample;
mod query_result;
mod range_series;
mod sample_value;

pub use annotated_query_result::AnnotatedQueryResult;
pub use annotations::Annotations;
pub use float_sample::FloatSample;
pub use histogram_quantile_repair::HistogramQuantileRepair;
pub use instant_sample::InstantSample;
pub use query_result::QueryResult;
pub use range_series::{RangeSeries, RangeSeriesSample};
pub use sample_value::SampleValue;
