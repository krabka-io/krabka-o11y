use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::Labels;

use super::{PromqlError, QueryResult, Result, labels_key};
use crate::{RangeSeries, SampleValue};

pub(crate) fn finalize_metric_names(result: &mut QueryResult) -> Result<()> {
    match result {
        QueryResult::InstantVector(samples) => {
            for sample in samples {
                finalize_name(&mut sample.labels, &mut sample.drop_name);
            }
        }
        QueryResult::RangeMatrix(series) => {
            for series in &mut *series {
                finalize_name(&mut series.labels, &mut series.drop_name);
            }
            merge_finalized_series(series)?;
        }
        QueryResult::Scalar { .. } | QueryResult::Str { .. } => {}
    }
    Ok(())
}

/// Pinned Prometheus cleanup merges matrix rows whose finalized labels agree
/// when timestamps within each sample kind are disjoint. Instant vectors still
/// reject duplicate labelsets.
fn merge_finalized_series(series: &mut Vec<RangeSeries>) -> Result<()> {
    let mut seen = BTreeSet::new();
    if series
        .iter()
        .all(|series| seen.insert(labels_key(&series.labels)))
    {
        return Ok(());
    }
    let mut indices = BTreeMap::new();
    let mut merged: Vec<RangeSeries> = Vec::with_capacity(seen.len());
    for mut series in std::mem::take(series) {
        let key = labels_key(&series.labels);
        if let Some(&index) = indices.get(&key) {
            let base: &mut RangeSeries = &mut merged[index];
            base.samples.append(&mut series.samples);
            base.start_timestamps_ms
                .append(&mut series.start_timestamps_ms);
        } else {
            indices.insert(key, merged.len());
            merged.push(series);
        }
    }
    for series in &mut merged {
        series.samples.sort_by_key(|(timestamp, value)| {
            (*timestamp, matches!(value, SampleValue::Histogram(_)))
        });
        if series.samples.windows(2).any(|pair| {
            pair[0].0 == pair[1].0
                && matches!(&pair[0].1, SampleValue::Histogram(_))
                    == matches!(&pair[1].1, SampleValue::Histogram(_))
        }) {
            return Err(PromqlError::Exec(format!(
                "vector cannot contain metrics with the same labelset: {}",
                labels_key(&series.labels)
            )));
        }
    }
    *series = merged;
    Ok(())
}

fn finalize_name(labels: &mut Labels, drop_name: &mut bool) {
    if *drop_name {
        *labels = labels
            .iter()
            .filter(|(name, _)| name.as_str() != "__name__")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        *drop_name = false;
    }
}

#[cfg(test)]
mod tests {
    use krabka_metrics::NativeHistogram;

    use super::*;

    fn series(name: &str, value: SampleValue) -> RangeSeries {
        let mut labels = Labels::new();
        labels.insert("__name__", name);
        RangeSeries {
            labels,
            drop_name: true,
            start_timestamps_ms: BTreeMap::from([(10, 1)]),
            samples: vec![(10, value)],
        }
    }

    #[test]
    fn finalized_matrix_collision_checks_preserve_sample_kinds() {
        let histogram = SampleValue::Histogram(NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: krabka_metrics::ResetHint::No,
            zero_threshold: 0.001,
            zero_count: 2.0,
            count: 2.0,
            sum: 0.0,
            positive_spans: vec![],
            positive_counts: vec![],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        });
        // The pinned merge checks floats and histograms separately: their
        // timestamps may coincide in the same finalized matrix row.
        let mut result = QueryResult::RangeMatrix(vec![
            series("a", histogram.clone()),
            series("b", SampleValue::Float(2.0)),
        ]);
        assert2::assert!(finalize_metric_names(&mut result).is_ok());
        let QueryResult::RangeMatrix(merged) = result else {
            panic!("matrix")
        };
        assert2::assert!(merged.len() == 1);
        assert2::assert!(merged[0].labels.is_empty());
        assert2::assert!(!merged[0].drop_name);
        assert2::assert!(merged[0].start_timestamps_ms == BTreeMap::from([(10, 1)]));
        assert2::assert!(
            merged[0].samples == vec![(10, SampleValue::Float(2.0)), (10, histogram.clone())]
        );
        for value in [SampleValue::Float(2.0), histogram] {
            let mut result =
                QueryResult::RangeMatrix(vec![series("a", value.clone()), series("b", value)]);
            assert2::assert!(matches!(
                finalize_metric_names(&mut result),
                Err(PromqlError::Exec(_))
            ));
        }
    }
}
