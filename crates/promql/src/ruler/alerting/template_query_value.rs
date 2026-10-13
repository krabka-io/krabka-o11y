use std::{collections::BTreeMap, sync::Arc};

use krabka_logql::TemplateData;

use super::{QueryResult, SampleValue};
use crate::engine::template_histogram_value;

pub(crate) fn template_query_value(result: QueryResult) -> TemplateData {
    let samples = match result {
        QueryResult::InstantVector(samples) => samples,
        QueryResult::Scalar { ts_ms, value } => vec![crate::InstantSample {
            labels: crate::PromqlLabels::new(),
            ts_ms,
            value: SampleValue::Float(value),
            drop_name: false,
        }],
        QueryResult::RangeMatrix(_) | QueryResult::Str { .. } => {
            return TemplateData::QueryError("rule result is not a vector or scalar".into());
        }
    };
    TemplateData::QueryResult(
        samples
            .into_iter()
            .map(|sample| {
                let value = template_sample_value(sample.value);
                let mut labels = BTreeMap::new();
                for pair in sample.labels.iter() {
                    labels.insert(pair.name.to_string(), pair.value.as_bytes().to_vec());
                }
                let mut row = BTreeMap::new();
                row.insert("Labels".to_owned(), TemplateData::ByteLabels(labels));
                row.insert("Value".to_owned(), value);
                TemplateData::Sample(Arc::new(row))
            })
            .collect::<Vec<_>>()
            .into(),
    )
}

pub(crate) fn template_sample_value(value: SampleValue) -> TemplateData {
    match value {
        SampleValue::Float(value) => TemplateData::Float(value),
        SampleValue::Histogram(histogram) => template_histogram_value(histogram),
    }
}

#[cfg(test)]
mod tests {
    use super::{Arc, QueryResult, TemplateData, template_query_value};

    #[test]
    fn scalar_queries_become_fresh_named_samples_and_other_result_kinds_are_errors() {
        let scalar = QueryResult::Scalar {
            ts_ms: 60_000,
            value: 7.0,
        };
        let TemplateData::QueryResult(first) = template_query_value(scalar.clone()) else {
            panic!("expected named query result")
        };
        let TemplateData::QueryResult(second) = template_query_value(scalar) else {
            panic!("expected named query result")
        };
        let first = first.snapshot();
        let second = second.snapshot();
        assert2::assert!(first.len() == 1 && second.len() == 1);
        let TemplateData::Sample(first_sample) = &first[0] else {
            panic!("expected source sample pointer")
        };
        let TemplateData::Sample(second_sample) = &second[0] else {
            panic!("expected source sample pointers")
        };
        assert2::assert!(
            matches!(first_sample.get("Labels"), Some(TemplateData::ByteLabels(labels)) if labels.is_empty())
        );
        assert2::assert!(
            matches!(first_sample.get("Value"), Some(TemplateData::Float(value)) if value.to_bits() == 7.0_f64.to_bits())
        );
        assert2::assert!(!Arc::ptr_eq(first_sample, second_sample));
        assert2::assert!(Arc::ptr_eq(first_sample, &Arc::clone(first_sample)));
        for result in [
            QueryResult::RangeMatrix(Vec::new()),
            QueryResult::Str {
                ts_ms: 60_000,
                value: "not a vector".into(),
            },
        ] {
            assert2::assert!(
                matches!(template_query_value(result), TemplateData::QueryError(error) if error == "rule result is not a vector or scalar")
            );
        }
    }
    #[test]
    fn query_histograms_preserve_fields_buckets_and_mixed_vector_rows() {
        use krabka_metrics::ResetHint;
        let histogram =
            crate::test_support::two_sided_histogram(8.0, 10.0, ResetHint::Gauge, Some(123));
        let samples = [
            crate::SampleValue::Histogram(histogram),
            crate::SampleValue::Float(7.0),
        ]
        .into_iter()
        .map(|value| crate::InstantSample {
            labels: crate::PromqlLabels::from_pairs([krabka_metrics::MetricLabelPair::new(
                "raw",
                crate::PromqlString::from(vec![0xff]),
            )]),
            ts_ms: 60_000,
            value,
            drop_name: false,
        })
        .collect();
        let result = template_query_value(QueryResult::InstantVector(samples));
        let TemplateData::QueryResult(rows) = &result else {
            panic!("expected query rows")
        };
        let rows = rows.snapshot();
        assert2::assert!(rows.len() == 2);
        let TemplateData::Sample(sample) = &rows[0] else {
            panic!("expected source sample")
        };
        let Some(TemplateData::FloatHistogram(histogram)) = sample.get("Value") else {
            panic!("expected native histogram")
        };
        let histogram = histogram.snapshot();
        assert2::assert!(histogram.counter_reset_hint == 3 && histogram.schema == 0);
        assert2::assert!(histogram.count.to_bits() == 8.0_f64.to_bits());
        assert2::assert!(histogram.sum.to_bits() == 10.0_f64.to_bits());
        assert2::assert!(histogram.zero_count.to_bits() == 2.0_f64.to_bits());
        assert2::assert!(
            histogram.positive_buckets == [1.0, 2.0] && histogram.negative_buckets == [1.0, 2.0]
        );
        assert2::assert!(histogram.custom_values.is_empty());
        #[derive(PartialEq)]
        struct BucketExpectation {
            lower: f64,
            upper: f64,
            count: f64,
            boundary_rule: u8,
        }
        let mut buckets = Vec::new();
        for bucket in &histogram.display_buckets {
            buckets.push(BucketExpectation {
                lower: bucket.lower,
                upper: bucket.upper,
                count: bucket.count,
                boundary_rule: bucket.boundary_rule,
            });
        }
        assert2::assert!(
            buckets
                == [
                    BucketExpectation {
                        lower: -2.0,
                        upper: -1.0,
                        count: 2.0,
                        boundary_rule: 1
                    },
                    BucketExpectation {
                        lower: -1.0,
                        upper: -0.5,
                        count: 1.0,
                        boundary_rule: 1
                    },
                    BucketExpectation {
                        lower: -0.001,
                        upper: 0.001,
                        count: 2.0,
                        boundary_rule: 3
                    },
                    BucketExpectation {
                        lower: 0.5,
                        upper: 1.0,
                        count: 1.0,
                        boundary_rule: 0
                    },
                    BucketExpectation {
                        lower: 1.0,
                        upper: 2.0,
                        count: 2.0,
                        boundary_rule: 0
                    },
                ]
        );
        let format = krabka_logql::LineFormat::new_prometheus(r#"{{ $h := query "hist" | first | value }}{{ $h.Count }}/{{ $h.Sum }}/{{ $h.Schema }}/{{ $h.UsesCustomBuckets }}/{{ $h.String }}/{{ query "hist" | first | label "raw" }}"#).unwrap();
        let rendered = format
            .render_prometheus_bytes(
                &std::collections::BTreeMap::new(),
                &[result.clone(), result.clone()],
                60_000,
            )
            .unwrap();
        let mut expected = b"8/10/0/false/{count:8, sum:10, [-2,-1):2, [-1,-0.5):1, [-0.001,0.001]:2, (0.5,1]:1, (1,2]:2}/".to_vec();
        expected.push(0xff);
        assert2::assert!(rendered == expected);
        let numeric = krabka_logql::LineFormat::new_prometheus(
            r#"{{ query "hist" | first | value | humanize }}"#,
        )
        .unwrap();
        assert2::assert!(matches!(
            numeric.render_prometheus_bytes(&std::collections::BTreeMap::new(), &[result], 60_000),
            Err(krabka_logql::TemplateRenderError::Execution(_))
        ));
    }
}
