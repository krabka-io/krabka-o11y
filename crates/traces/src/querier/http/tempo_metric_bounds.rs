use super::TraceMetricsResponse;

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct TempoMetricBounds {
    pub(crate) scan_start_ns: i64,
    pub(crate) scan_end_ns: i64,
    sample_shift: i64,
}

/// Adapt Tempo's aligned, right-closed buckets to the engine's inclusive scan.
/// The pinned Tempo `AlignRequest` adds an initial bucket when start > step.
pub(crate) fn tempo_metric_bounds(
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
) -> Result<Option<TempoMetricBounds>, &'static str> {
    if step_ns <= 0 {
        return Err("step must be positive");
    }
    if end_ns < start_ns {
        return Err("end must be >= start");
    }
    let overflow = "aligned metrics range overflows nanosecond timestamps";
    let mut start = start_ns
        .checked_sub(start_ns.rem_euclid(step_ns))
        .ok_or(overflow)?;
    if start > step_ns {
        start = start.checked_sub(step_ns).ok_or(overflow)?;
    }
    let remainder = end_ns.rem_euclid(step_ns);
    let end = end_ns
        .checked_add(if remainder == 0 {
            0
        } else {
            step_ns - remainder
        })
        .ok_or(overflow)?;
    let duration = end.checked_sub(start).ok_or(overflow)?;
    if duration == 0 {
        return Ok(None);
    }
    Ok(Some(TempoMetricBounds {
        scan_start_ns: start.checked_add(1).ok_or(overflow)?,
        scan_end_ns: end,
        sample_shift: step_ns - 1,
    }))
}

impl TempoMetricBounds {
    /// Move samples to right boundaries and use Tempo's aligned fetch precision.
    pub(crate) fn shift_points(
        &self,
        response: &mut TraceMetricsResponse,
    ) -> Result<(), &'static str> {
        let step_ns = self.sample_shift + 1;
        // Pinned vParquet5 selects the least precise start column whose
        // granularity divides the aligned query step. Reconstructed exemplar
        // times are the right boundary of that stored interval.
        let precision = [3600_i64, 300, 60, 15]
            .into_iter()
            .map(|seconds| seconds * 1_000_000_000)
            .find(|precision| step_ns % precision == 0);
        for series in &mut response.series {
            for point in &mut series.points {
                point.timestamp = point
                    .timestamp
                    .checked_add(self.sample_shift)
                    .ok_or("metric sample timestamp overflow")?;
            }
            if let Some(precision) = precision {
                for exemplar in &mut series.exemplars {
                    let remainder = exemplar.timestamp_ns.rem_euclid(precision);
                    if remainder != 0 {
                        exemplar.timestamp_ns = exemplar
                            .timestamp_ns
                            .checked_add(precision - remainder)
                            .ok_or("metric exemplar timestamp overflow")?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use assert2::assert;
    use krabka_traceql::{
        EngineOpts, InMemorySpanStore, InputSpan, MetricPoint, TraceMetricSeries, TraceqlEngine,
    };
    use krabka_units::{Time, convert::TimeExt as _};
    use serde_json::json;

    use super::*;

    struct BoundsCase {
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        expected: Option<TempoMetricBounds>,
    }

    #[allow(clippy::struct_field_names)]
    struct BoundsInput {
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    }

    fn expected_bounds(
        scan_start_ns: i64,
        scan_end_ns: i64,
        sample_shift: i64,
    ) -> TempoMetricBounds {
        TempoMetricBounds {
            scan_start_ns,
            scan_end_ns,
            sample_shift,
        }
    }

    #[test]
    fn aligned_bounds_cover_rounded_edges_and_initial_bucket() {
        for case in [
            BoundsCase {
                start_ns: 20,
                end_ns: 40,
                step_ns: 10,
                expected: Some(expected_bounds(11, 40, 9)),
            },
            BoundsCase {
                start_ns: 21,
                end_ns: 41,
                step_ns: 10,
                expected: Some(expected_bounds(11, 50, 9)),
            },
            BoundsCase {
                start_ns: 10,
                end_ns: 20,
                step_ns: 10,
                expected: Some(expected_bounds(11, 20, 9)),
            },
            BoundsCase {
                start_ns: 0,
                end_ns: 20,
                step_ns: 10,
                expected: Some(expected_bounds(1, 20, 9)),
            },
            BoundsCase {
                start_ns: 0,
                end_ns: 0,
                step_ns: 10,
                expected: None,
            },
            BoundsCase {
                start_ns: 10,
                end_ns: 10,
                step_ns: 10,
                expected: None,
            },
            BoundsCase {
                start_ns: 20,
                end_ns: 20,
                step_ns: 10,
                expected: Some(expected_bounds(11, 20, 9)),
            },
            BoundsCase {
                start_ns: -11,
                end_ns: -1,
                step_ns: 10,
                expected: Some(expected_bounds(-19, 0, 9)),
            },
            BoundsCase {
                start_ns: i64::MAX,
                end_ns: i64::MAX,
                step_ns: 1,
                expected: Some(expected_bounds(i64::MAX, i64::MAX, 0)),
            },
        ] {
            assert!(
                tempo_metric_bounds(case.start_ns, case.end_ns, case.step_ns).unwrap()
                    == case.expected,
                "start={}, end={}, step={}",
                case.start_ns,
                case.end_ns,
                case.step_ns
            );
        }
        for input in [
            BoundsInput {
                start_ns: 20,
                end_ns: 10,
                step_ns: 10,
            },
            BoundsInput {
                start_ns: 0,
                end_ns: 20,
                step_ns: 0,
            },
            BoundsInput {
                start_ns: 0,
                end_ns: 20,
                step_ns: -1,
            },
            BoundsInput {
                start_ns: i64::MAX - 1,
                end_ns: i64::MAX,
                step_ns: 10,
            },
            BoundsInput {
                start_ns: i64::MIN,
                end_ns: 0,
                step_ns: 10,
            },
            BoundsInput {
                start_ns: i64::MIN,
                end_ns: 0,
                step_ns: 1,
            },
        ] {
            assert!(tempo_metric_bounds(input.start_ns, input.end_ns, input.step_ns).is_err());
        }
    }

    #[tokio::test]
    async fn engine_adapter_excludes_left_edges_and_includes_right_edges() {
        let mut store = InMemorySpanStore::new();
        let timestamps = [10, 11, 20, 21, 30, 31, 40, 41];
        let spans = (0..timestamps.len())
            .map(|index| InputSpan {
                trace_id: [1; 16],
                span_id: [u8::try_from(index + 1).unwrap(); 8],
                parent_span_id: None,
                name: "boundary".into(),
                kind: 0,
                start_unix_nano: timestamps[index],
                duration: Time::from_nanos(1),
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                attrs: Vec::new(),
                events: Vec::new(),
                links: Vec::new(),
            })
            .collect();
        store.push_trace("tenant", "boundaries", "root", spans);
        let engine = TraceqlEngine::new(
            Arc::new(store),
            EngineOpts {
                max_exemplars: 0,
                ..EngineOpts::default()
            },
        );
        let bounds = tempo_metric_bounds(20, 40, 10).unwrap().unwrap();
        let mut response = engine
            .query_range(
                "tenant",
                "{} | count_over_time()",
                bounds.scan_start_ns,
                bounds.scan_end_ns,
                10,
            )
            .await
            .unwrap();
        bounds.shift_points(&mut response).unwrap();
        // (10,20], (20,30], (30,40] each contain exactly two assigned spans.
        assert!(
            response
                == TraceMetricsResponse {
                    series: vec![TraceMetricSeries {
                        label_types: BTreeMap::default(),
                        labels: Vec::new(),
                        points: vec![
                            MetricPoint::new(20, 2.0),
                            MetricPoint::new(30, 2.0),
                            MetricPoint::new(40, 2.0)
                        ],
                        exemplars: Vec::new(),
                    }],
                }
        );
    }

    #[test]
    fn sample_shift_preserves_exemplars_and_rejects_overflow() {
        let mut response: TraceMetricsResponse = TraceMetricsResponse {
            series: vec![serde_json::from_value(json!({
                "labels": [["service", "checkout"]], "points": [[11, 3.0], [21, 0.0]],
                "exemplars": [{"labels": [["trace:id", "01"]], "value": 3.0, "timestamp_ns": 17}]
            }))
            .unwrap()],
        };
        let mut expected = response.clone();
        expected.series[0].points = vec![MetricPoint::new(20, 3.0), MetricPoint::new(30, 0.0)];
        let bounds = tempo_metric_bounds(20, 40, 10).unwrap().unwrap();
        bounds.shift_points(&mut response).unwrap();
        assert!(response == expected);
        response.series[0].points[0].timestamp = i64::MAX;
        assert!(bounds.shift_points(&mut response).is_err());
    }

    #[test]
    fn exemplar_precision_uses_supported_step_divisors_and_right_boundaries() {
        struct ExemplarCase {
            step_seconds: i64,
            timestamp: i64,
            expected_timestamp: i64,
        }
        for case in [
            ExemplarCase {
                step_seconds: 30,
                timestamp: 100_000_000,
                expected_timestamp: 15_000_000_000,
            },
            ExemplarCase {
                step_seconds: 30,
                timestamp: 15_000_000_000,
                expected_timestamp: 15_000_000_000,
            },
            ExemplarCase {
                step_seconds: 30,
                timestamp: 15_000_000_001,
                expected_timestamp: 30_000_000_000,
            },
            ExemplarCase {
                step_seconds: 30,
                timestamp: -1,
                expected_timestamp: 0,
            },
            ExemplarCase {
                step_seconds: 30,
                timestamp: -15_000_000_001,
                expected_timestamp: -15_000_000_000,
            },
            ExemplarCase {
                step_seconds: 60,
                timestamp: 100_000_000,
                expected_timestamp: 60_000_000_000,
            },
            ExemplarCase {
                step_seconds: 300,
                timestamp: 100_000_000,
                expected_timestamp: 300_000_000_000,
            },
            ExemplarCase {
                step_seconds: 3600,
                timestamp: 100_000_000,
                expected_timestamp: 3_600_000_000_000,
            },
            ExemplarCase {
                step_seconds: 31,
                timestamp: 100_000_000,
                expected_timestamp: 100_000_000,
            },
        ] {
            let step = case.step_seconds * 1_000_000_000;
            let bounds = tempo_metric_bounds(step + 1, step * 3 - 1, step)
                .unwrap()
                .unwrap();
            let mut response = TraceMetricsResponse {
                series: vec![serde_json::from_value(json!({
                    "labels": [], "points": [], "exemplars":[{"labels": [["trace:id", "01"]], "value": 3.0, "timestamp_ns": case.timestamp}]
                })).unwrap()],
            };
            let mut expected_response = response.clone();
            expected_response.series[0].exemplars[0].timestamp_ns = case.expected_timestamp;
            bounds.shift_points(&mut response).unwrap();
            assert!(
                response == expected_response,
                "step={step}, timestamp={}",
                case.timestamp
            );
            response.series[0].exemplars[0].timestamp_ns = i64::MAX;
            assert!(bounds.shift_points(&mut response).is_err() == (case.step_seconds != 31));
        }
    }
}
