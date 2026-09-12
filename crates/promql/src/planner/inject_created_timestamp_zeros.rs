use super::TimedValue;

/// Inserts synthetic zero samples for valid counter start timestamps.
///
/// A start timestamp is usable when it lies inside the range and strictly
/// between the preceding real sample (if any) and the sample that carries it.
/// Repeated cumulative-counter start timestamps therefore inject exactly one
/// zero, while a changed timestamp between samples makes an otherwise hidden
/// reset visible to the ordinary rate/reset kernels.
pub(crate) fn inject_created_timestamp_zeros(samples: &mut Vec<TimedValue>, range_start_ms: i64) {
    if samples.is_empty() {
        return;
    }

    let mut with_zeros = Vec::with_capacity(samples.len().saturating_mul(2));
    let mut previous_timestamp_ms = None;
    for sample in samples.drain(..) {
        if let Some(start_timestamp_ms) = sample.start_timestamp_ms
            && start_timestamp_ms > range_start_ms
            && start_timestamp_ms < sample.ts_ms
            && previous_timestamp_ms.is_none_or(|previous| start_timestamp_ms > previous)
        {
            with_zeros.push(TimedValue {
                ts_ms: start_timestamp_ms,
                value: 0.0,
                start_timestamp_ms: None,
            });
        }
        previous_timestamp_ms = Some(sample.ts_ms);
        with_zeros.push(sample);
    }
    *samples = with_zeros;
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn injects_series_start_and_changed_start_but_not_repeated_or_overlapping_starts() {
        let mut samples = vec![
            TimedValue {
                ts_ms: 100,
                value: 5.0,
                start_timestamp_ms: Some(50),
            },
            TimedValue {
                ts_ms: 200,
                value: 9.0,
                start_timestamp_ms: Some(50),
            },
            TimedValue {
                ts_ms: 300,
                value: 12.0,
                start_timestamp_ms: Some(250),
            },
            TimedValue {
                ts_ms: 400,
                value: 15.0,
                start_timestamp_ms: Some(150),
            },
        ];

        inject_created_timestamp_zeros(&mut samples, 0);

        assert!(
            samples
                .iter()
                .map(|sample| (sample.ts_ms, sample.value))
                .collect::<Vec<_>>()
                == vec![
                    (50, 0.0),
                    (100, 5.0),
                    (200, 9.0),
                    (250, 0.0),
                    (300, 12.0),
                    (400, 15.0)
                ]
        );
    }
}
