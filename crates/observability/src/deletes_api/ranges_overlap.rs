use super::*;

pub(crate) fn ranges_overlap(left: TimeRange, right: TimeRange) -> bool {
    left.end_ns >= right.start_ns && left.start_ns <= right.end_ns
}
