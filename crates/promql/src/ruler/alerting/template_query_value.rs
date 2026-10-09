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
                TemplateData::Sample(Arc::new(BTreeMap::from([
                    (
                        "Labels".to_owned(),
                        TemplateData::ByteLabels(
                            sample
                                .labels
                                .iter()
                                .map(|(name, value)| (name.clone(), value.as_bytes().to_vec()))
                                .collect(),
                        ),
                    ),
                    ("Value".to_owned(), value),
                ])))
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
        let (TemplateData::Sample(first), TemplateData::Sample(second)) = (&first[0], &second[0])
        else {
            panic!("expected source sample pointers")
        };
        assert2::assert!(
            matches!(first.get("Labels"), Some(TemplateData::ByteLabels(labels)) if labels.is_empty())
        );
        assert2::assert!(
            matches!(first.get("Value"), Some(TemplateData::Float(value)) if value.to_bits() == 7.0_f64.to_bits())
        );
        assert2::assert!(!Arc::ptr_eq(first, second));
        assert2::assert!(Arc::ptr_eq(first, &Arc::clone(first)));
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
        use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
        let histogram = NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::Gauge,
            zero_threshold: 0.001,
            zero_count: 2.0,
            count: 8.0,
            sum: 10.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.0, 2.0],
            negative_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            negative_counts: vec![1.0, 2.0],
            custom_values: None,
            start_timestamp_ms: Some(123),
        };
        let samples = [
            crate::SampleValue::Histogram(histogram),
            crate::SampleValue::Float(7.0),
        ]
        .into_iter()
        .map(|value| crate::InstantSample {
            labels: crate::PromqlLabels::from_pairs([(
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
        assert2::assert!(
            (
                histogram.count.to_bits(),
                histogram.sum.to_bits(),
                histogram.zero_count.to_bits()
            ) == (8.0_f64.to_bits(), 10.0_f64.to_bits(), 2.0_f64.to_bits())
        );
        assert2::assert!(
            histogram.positive_buckets == [1.0, 2.0] && histogram.negative_buckets == [1.0, 2.0]
        );
        assert2::assert!(histogram.custom_values.is_empty());
        let buckets = histogram
            .display_buckets
            .iter()
            .map(|bucket| {
                (
                    bucket.lower,
                    bucket.upper,
                    bucket.count,
                    bucket.boundary_rule,
                )
            })
            .collect::<Vec<_>>();
        assert2::assert!(
            buckets
                == [
                    (-2.0, -1.0, 2.0, 1),
                    (-1.0, -0.5, 1.0, 1),
                    (-0.001, 0.001, 2.0, 3),
                    (0.5, 1.0, 1.0, 0),
                    (1.0, 2.0, 2.0, 0)
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
