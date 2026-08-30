use super::*;

pub(crate) fn eval_times(range: TimeRange, step_ns: i64) -> Vec<i64> {
    let mut times = Vec::new();
    let mut time = range.start_ns;
    while time <= range.end_ns {
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
