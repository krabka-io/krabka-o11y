use std::collections::BTreeMap;

use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::{NativeHistogram, ResetHint};
use krabka_units::prelude::*;
use promql_parser::parser::{Expr, MatrixSelector, SubqueryExpr, VectorSelector};

use super::{
    AtModifierBounds, PromqlEngine, RangeEval, current_at_modifier_bounds,
    histogram::native_histogram_detect_reset,
    histogram_stats_enabled,
    planner_support::validate_extended_selector_modifier,
    range_functions::{align_subquery_start, instant_smoothed_boundary_value},
    row_cache::HistogramRow,
    selector::{apply_selector_time_modifier, label_matcher_sets, selector_duration},
};
use crate::{
    PromqlError,
    error::Result,
    extension::is_stale_nan,
    planner::{ExtendedSelectorExpr, ExtendedSelectorModifier, TimedValue},
    result::{InstantSample, QueryResult, RangeSeries, SampleValue},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    pub(super) async fn eval_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            current_at_modifier_bounds(),
        )?;
        let start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta.millis_i64());
        let matcher_sets = label_matcher_sets(selector);
        if let Some(series) = self
            .latest_labeled_series(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?
        {
            // Sum and average use this evaluator to preserve compensated
            // accumulation, rather than the operator selector plan.
            let samples = series
                .into_iter()
                .filter_map(|series| {
                    let sample = series.samples.last()?;
                    if is_stale_nan(sample.value) {
                        return None;
                    }
                    Some(InstantSample {
                        labels: (*series.labels).clone(),
                        ts_ms: sample.ts_ms,
                        value: SampleValue::Float(sample.value),
                        drop_name: false,
                    })
                })
                .collect();
            return Ok(QueryResult::InstantVector(samples));
        }
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        let mut hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, start_ms, eval_time_ms)
            .await?;
        recompute_histogram_stats(&mut hist_rows);

        let mut latest_by_fp: BTreeMap<SeriesFingerprint, (i64, SampleValue)> = BTreeMap::new();
        for row in rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            latest_by_fp
                .entry(row.fp)
                .and_modify(|latest| {
                    if row.ts_ms > latest.0 {
                        *latest = (row.ts_ms, SampleValue::Float(row.value));
                    }
                })
                .or_insert((row.ts_ms, SampleValue::Float(row.value)));
        }
        for row in hist_rows {
            if row.ts_ms <= start_ms || row.ts_ms > eval_time_ms {
                continue;
            }
            latest_by_fp
                .entry(row.fp)
                .and_modify(|latest| {
                    if row.ts_ms > latest.0 {
                        *latest = (row.ts_ms, SampleValue::Histogram(row.hist.clone()));
                    }
                })
                .or_insert((row.ts_ms, SampleValue::Histogram(row.hist)));
        }

        let samples = latest_by_fp
            .into_iter()
            .filter_map(|(fp, (ts_ms, value))| {
                if matches!(&value, SampleValue::Float(value) if is_stale_nan(*value)) {
                    return None;
                }
                labels_by_fp.get(&fp).map(|labels| InstantSample {
                    labels: (**labels).clone(),
                    ts_ms,
                    value,
                    drop_name: false,
                })
            })
            .collect();
        Ok(QueryResult::InstantVector(samples))
    }

    pub(super) async fn eval_smoothed_instant_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let eval_time_ms = apply_selector_time_modifier(
            time_ms,
            selector.at.as_ref(),
            selector.offset.as_ref(),
            current_at_modifier_bounds(),
        )?;
        let scan_start_ms = eval_time_ms.saturating_sub(self.opts.lookback_delta.millis_i64());
        let scan_end_ms = eval_time_ms.saturating_add(self.opts.lookback_delta.millis_i64());
        let matcher_sets = label_matcher_sets(selector);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;

        let mut hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        recompute_histogram_stats(&mut hist_rows);
        let mut rows_by_fp: BTreeMap<SeriesFingerprint, Vec<(i64, SampleValue)>> = BTreeMap::new();
        for row in rows {
            if row.ts_ms > scan_start_ms && row.ts_ms <= scan_end_ms && !is_stale_nan(row.value) {
                rows_by_fp
                    .entry(row.fp)
                    .or_default()
                    .push((row.ts_ms, SampleValue::Float(row.value)));
            }
        }
        for row in hist_rows {
            if row.ts_ms > scan_start_ms && row.ts_ms <= scan_end_ms {
                rows_by_fp
                    .entry(row.fp)
                    .or_default()
                    .push((row.ts_ms, SampleValue::Histogram(row.hist)));
            }
        }
        let samples = rows_by_fp
            .into_iter()
            .filter_map(|(fp, mut rows)| {
                let labels = labels_by_fp.get(&fp)?;
                let metric = labels.get("__name__").unwrap_or("");
                rows.sort_by_key(|(timestamp, _)| *timestamp);
                let floats = rows
                    .iter()
                    .any(|(_, value)| matches!(value, SampleValue::Float(_)));
                let histograms = rows
                    .iter()
                    .any(|(_, value)| matches!(value, SampleValue::Histogram(_)));
                if floats && histograms {
                    super::annotations::emit_warning(
                        super::annotations::mixed_floats_histograms_warning(metric),
                    );
                    return None;
                }
                let value = if floats {
                    let timestamps = rows
                        .iter()
                        .map(|(timestamp, _)| *timestamp)
                        .collect::<Vec<_>>();
                    let values = rows
                        .iter()
                        .map(|(_, value)| match value {
                            SampleValue::Float(value) => *value,
                            SampleValue::Histogram(_) => unreachable!("float-only"),
                        })
                        .collect::<Vec<_>>();
                    SampleValue::Float(instant_smoothed_boundary_value(
                        &timestamps,
                        &values,
                        eval_time_ms,
                    )?)
                } else {
                    let points = rows
                        .into_iter()
                        .map(|(timestamp, value)| match value {
                            SampleValue::Histogram(histogram) => (timestamp, histogram),
                            SampleValue::Float(_) => unreachable!("histogram-only"),
                        })
                        .collect::<Vec<_>>();
                    let index = points.partition_point(|(timestamp, _)| *timestamp < eval_time_ms);
                    let histogram = if points
                        .get(index)
                        .is_some_and(|(timestamp, _)| *timestamp == eval_time_ms)
                    {
                        points[index].1.clone()
                    } else if index > 0 && index < points.len() {
                        let counter = points[index - 1].1.reset_hint != ResetHint::Gauge
                            || points[index].1.reset_hint != ResetHint::Gauge;
                        super::range_functions::interpolate_histogram(
                            &points[index - 1],
                            &points[index],
                            eval_time_ms,
                            counter,
                            metric,
                        )?
                    } else if index > 0 {
                        let mut previous = points[index - 1].1.clone();
                        previous.reset_hint = ResetHint::Unknown;
                        previous
                    } else {
                        return None;
                    };
                    SampleValue::Histogram(histogram)
                };
                Some(InstantSample {
                    labels: (**labels).clone(),
                    ts_ms: time_ms,
                    value,
                    drop_name: false,
                })
            })
            .collect();
        Ok(QueryResult::InstantVector(samples))
    }

    pub(super) async fn eval_matrix_selector(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        start_ms: i64,
        end_ms: i64,
        modifier: Option<ExtendedSelectorModifier>,
    ) -> Result<Vec<RangeSeries>> {
        self.eval_matrix_selector_inner(tenant, selector, start_ms, end_ms, modifier, false)
            .await
    }

    async fn eval_matrix_selector_inner(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        start_ms: i64,
        end_ms: i64,
        modifier: Option<ExtendedSelectorModifier>,
        _inject_zeros: bool,
    ) -> Result<Vec<RangeSeries>> {
        let range = selector_duration(selector.range)?;
        let bounds = AtModifierBounds { start_ms, end_ms };
        let eval_start_ms = apply_selector_time_modifier(
            start_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            Some(bounds),
        )?;
        let eval_end_ms = apply_selector_time_modifier(
            end_ms,
            selector.vs.at.as_ref(),
            selector.vs.offset.as_ref(),
            Some(bounds),
        )?;
        let range_start_ms = eval_start_ms.saturating_sub(range.millis_i64());
        let scan_start_ms = match modifier {
            Some(ExtendedSelectorModifier::Anchored | ExtendedSelectorModifier::Smoothed) => {
                range_start_ms.saturating_sub(self.opts.lookback_delta.millis_i64())
            }
            None => range_start_ms,
        };
        let scan_end_ms = match modifier {
            Some(ExtendedSelectorModifier::Smoothed) => {
                eval_end_ms.saturating_add(self.opts.lookback_delta.millis_i64())
            }
            Some(ExtendedSelectorModifier::Anchored) | None => eval_end_ms,
        };
        let matcher_sets = label_matcher_sets(&selector.vs);
        let labels_by_fp = self
            .labels_by_fingerprint_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let rows = self
            .scan_float_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        let mut hist_rows = self
            .scan_histogram_row_sets(tenant, &matcher_sets, scan_start_ms, scan_end_ms)
            .await?;
        recompute_histogram_stats(&mut hist_rows);

        let mut float_samples_by_fp: BTreeMap<SeriesFingerprint, Vec<TimedValue>> = BTreeMap::new();
        for row in rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms || is_stale_nan(row.value) {
                continue;
            }
            float_samples_by_fp
                .entry(row.fp)
                .or_default()
                .push(TimedValue {
                    ts_ms: row.ts_ms,
                    value: row.value,
                    start_timestamp_ms: row.start_timestamp_ms,
                });
        }

        let mut starts_by_fp = BTreeMap::new();
        let mut samples_by_fp: BTreeMap<SeriesFingerprint, BTreeMap<i64, SampleValue>> =
            BTreeMap::new();
        for (fp, mut samples) in float_samples_by_fp {
            samples.sort_unstable_by_key(|sample| sample.ts_ms);
            starts_by_fp.insert(
                fp,
                samples
                    .iter()
                    .filter_map(|sample| {
                        sample.start_timestamp_ms.map(|start| (sample.ts_ms, start))
                    })
                    .collect(),
            );
            samples_by_fp.entry(fp).or_default().extend(
                samples
                    .into_iter()
                    .map(|sample| (sample.ts_ms, SampleValue::Float(sample.value))),
            );
        }
        for row in hist_rows {
            if row.ts_ms <= scan_start_ms || row.ts_ms > scan_end_ms {
                continue;
            }
            if let Some(start) = row.hist.start_timestamp_ms {
                starts_by_fp
                    .entry(row.fp)
                    .or_insert_with(BTreeMap::new)
                    .insert(row.ts_ms, start);
            }
            samples_by_fp
                .entry(row.fp)
                .or_default()
                .insert(row.ts_ms, SampleValue::Histogram(row.hist));
        }

        let mut out = Vec::new();
        for (fp, samples) in samples_by_fp {
            let Some(labels) = labels_by_fp.get(&fp) else {
                continue;
            };
            out.push(RangeSeries {
                drop_name: false,
                start_timestamps_ms: starts_by_fp.remove(&fp).unwrap_or_default(),
                labels: (**labels).clone(),
                samples: samples.into_iter().collect(),
            });
        }
        Ok(out)
    }

    pub(super) async fn eval_subquery(
        &self,
        tenant: &str,
        subquery: &SubqueryExpr,
        time_ms: i64,
    ) -> Result<Vec<RangeSeries>> {
        let range = selector_duration(subquery.range)?;
        let step = match subquery.step {
            Some(step) => selector_duration(step)?,
            None => self.opts.eval_interval,
        };
        if step <= Time::ZERO {
            return Err(PromqlError::Plan(
                "subquery step must be positive".to_string(),
            ));
        }
        let end_ms = apply_selector_time_modifier(
            time_ms,
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            None,
        )?;
        let start_ms = align_subquery_start(end_ms.saturating_sub(range.millis_i64()), step);
        // Evaluate the subquery's inner instant expression over its sub-grid
        // through the operator planner (the sole evaluation engine). The planner
        // is total, so it produces a result for every plannable inner; an
        // `Ok(None)` would be a planner bug, surfaced as an internal error.
        self.eval_range_via_planner(tenant, &subquery.expr, start_ms, end_ms, step)
            .await?
            .ok_or_else(|| {
                PromqlError::Plan("planner returned no result for a subquery inner".to_string())
            })
    }

    pub(super) async fn eval_range_arg(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
        function_name: &str,
    ) -> Result<RangeEval> {
        let mut expr = expr;
        let mut modifier = None;
        loop {
            match expr {
                Expr::Paren(paren) => expr = &paren.expr,
                Expr::Extension(extension) => {
                    let Some(extended) = extension
                        .expr
                        .as_any()
                        .downcast_ref::<ExtendedSelectorExpr>()
                    else {
                        return Err(PromqlError::Plan(format!(
                            "{function_name} expects a range-vector selector"
                        )));
                    };
                    validate_extended_selector_modifier(function_name, extended.modifier())?;
                    modifier = Some(extended.modifier());
                    let Some(child) = extended.child() else {
                        return Err(PromqlError::Plan(format!(
                            "{function_name} expects a range-vector selector"
                        )));
                    };
                    expr = child;
                }
                _ => break,
            }
        }

        match expr {
            Expr::MatrixSelector(selector) => {
                let range = selector_duration(selector.range)?;
                let end_ms = apply_selector_time_modifier(
                    time_ms,
                    selector.vs.at.as_ref(),
                    selector.vs.offset.as_ref(),
                    None,
                )?;
                let series = self
                    .eval_matrix_selector_inner(
                        tenant,
                        selector,
                        time_ms,
                        time_ms,
                        modifier,
                        modifier.is_none()
                            && matches!(function_name, "rate" | "increase" | "resets"),
                    )
                    .await?;
                Ok(RangeEval {
                    enable_type_and_unit_labels: self.opts.enable_type_and_unit_labels,
                    series,
                    end_ms,
                    range,
                    modifier,
                })
            }
            Expr::Subquery(subquery) => {
                let range = selector_duration(subquery.range)?;
                let end_ms = apply_selector_time_modifier(
                    time_ms,
                    subquery.at.as_ref(),
                    subquery.offset.as_ref(),
                    None,
                )?;
                let series = self.eval_subquery(tenant, subquery, time_ms).await?;
                Ok(RangeEval {
                    enable_type_and_unit_labels: self.opts.enable_type_and_unit_labels,
                    series,
                    end_ms,
                    range,
                    modifier,
                })
            }
            _ => Err(PromqlError::Plan(format!(
                "{function_name} expects a range-vector selector"
            ))),
        }
    }
}

fn recompute_histogram_stats(rows: &mut [HistogramRow]) {
    if !histogram_stats_enabled() {
        return;
    }
    rows.sort_unstable_by_key(|row| (row.fp, row.ts_ms));
    let mut previous = BTreeMap::<SeriesFingerprint, NativeHistogram>::new();
    for row in rows {
        if row.hist.reset_hint != ResetHint::Gauge {
            let mut current = row.hist.clone();
            current.reset_hint = ResetHint::Unknown;
            row.hist.reset_hint = previous.get(&row.fp).map_or(ResetHint::Unknown, |last| {
                if native_histogram_detect_reset(last, &current) {
                    ResetHint::Yes
                } else {
                    ResetHint::No
                }
            });
        }
        previous.insert(row.fp, row.hist.clone());
    }
}
