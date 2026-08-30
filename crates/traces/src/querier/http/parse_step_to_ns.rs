use super::{parse_seconds_to_ns, parse_go_duration_ns};

pub(crate) fn parse_step_to_ns(value: &str) -> Option<i64> {
    parse_seconds_to_ns(value).or_else(|| i64::try_from(parse_go_duration_ns(value).ok()?).ok())
}
