use super::{QueryError, TimeRange};

pub(crate) fn eval_times(range: TimeRange, step_ns: i64) -> Vec<i64> {
    let mut times = Vec::new();
    let align_up = |time: i64| {
        let remainder = time.rem_euclid(step_ns);
        if remainder == 0 {
            Some(time)
        } else {
            time.checked_add(step_ns - remainder)
        }
    };
    let Some(mut time) = align_up(range.start_ns) else {
        return times;
    };
    let Some(end) = align_up(range.end_ns) else {
        return times;
    };
    while time <= end {
        times.push(time);
        let Some(next) = time.checked_add(step_ns) else {
            break;
        };
        if next <= time {
            break;
        }
        time = next;
    }
    times
}

/// The evaluation grid of a range query, after rejecting a step that is not
/// positive.
pub(crate) fn checked_eval_times(range: TimeRange, step_ns: i64) -> Result<Vec<i64>, QueryError> {
    if step_ns <= 0 {
        return Err(QueryError::InvalidStep(step_ns));
    }
    Ok(eval_times(range, step_ns))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluation_grid_aligns_both_bounds_up_to_the_step() {
        assert_eq!(
            eval_times(
                TimeRange {
                    start_ns: 11,
                    end_ns: 29,
                },
                10,
            ),
            vec![20, 30]
        );
    }
}
