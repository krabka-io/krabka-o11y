//! Shared test helpers for this test area.
use super::*;

pub(crate) mod labels {
    use super::*;

    pub(crate) fn labels(metric: &str, job: &str) -> Labels {
        let mut labels = Labels::new();
        labels.insert("__name__", metric);
        labels.insert("job", job);
        labels
    }
}

pub(crate) mod parse_duration_supports_all_units_and_compounds_and_rejects_bad_input {
    use super::*;
    use crate::ruler::parse_duration;

    #[yare::parameterized(
    compound = { "1h30m", Some(millis(5_400_000)) },
    milliseconds = { "100ms", Some(millis(100)) },
    seconds = { "5s", Some(secs(5)) },
    weeks = { "1w", Some(days(7)) },
    years = { "1y", Some(days(365)) },
    zero = { "0", Some(Time::ZERO) },
    negative = { "-5m", None },
    empty = { "", None },
    unknown_unit = { "5x", None },
    invalid = { "abc", None },
)]
    pub(crate) fn parse_duration_supports_all_units_and_compounds_and_rejects_bad_input(
        input: &str,
        want: Option<Time>,
    ) {
        assert2::assert!(parse_duration(input).ok() == want);
    }
}
