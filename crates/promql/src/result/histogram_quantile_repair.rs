use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// Details of classic histogram monotonicity repairs, retained across query
/// steps, frontend splits, and cache entries. Float bit patterns preserve
/// infinite bucket bounds when this internal metadata is serialized as JSON.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct HistogramQuantileRepair {
    min_bucket_bits: u64,
    max_bucket_bits: u64,
    max_diff_bits: u64,
    count: u64,
    min_ts_ms: i64,
    max_ts_ms: i64,
}

impl HistogramQuantileRepair {
    pub(crate) fn new(min_bucket: f64, max_bucket: f64, max_diff: f64, timestamp_ms: i64) -> Self {
        Self {
            min_bucket_bits: min_bucket.to_bits(),
            max_bucket_bits: max_bucket.to_bits(),
            max_diff_bits: max_diff.to_bits(),
            count: 1,
            min_ts_ms: timestamp_ms,
            max_ts_ms: timestamp_ms,
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        self.min_bucket_bits = f64::from_bits(self.min_bucket_bits)
            .min(f64::from_bits(other.min_bucket_bits))
            .to_bits();
        self.max_bucket_bits = f64::from_bits(self.max_bucket_bits)
            .max(f64::from_bits(other.max_bucket_bits))
            .to_bits();
        self.max_diff_bits = f64::from_bits(self.max_diff_bits)
            .max(f64::from_bits(other.max_diff_bits))
            .to_bits();
        self.count += other.count;
        self.min_ts_ms = self.min_ts_ms.min(other.min_ts_ms);
        self.max_ts_ms = self.max_ts_ms.max(other.max_ts_ms);
    }

    pub(crate) fn render(&self, message: &str) -> Option<String> {
        let timestamp = |ms: i64| {
            OffsetDateTime::from_unix_timestamp(ms / 1000)
                .ok()?
                .format(&Rfc3339)
                .ok()
        };
        let start = timestamp(self.min_ts_ms)?;
        let end = timestamp(self.max_ts_ms)?;
        let (base, position) = message
            .rsplit_once(" (")
            .filter(|(_, position)| {
                position
                    .strip_suffix(')')
                    .and_then(|value| value.split_once(':'))
                    .is_some_and(|(line, column)| {
                        !line.is_empty()
                            && !column.is_empty()
                            && line.bytes().all(|byte| byte.is_ascii_digit())
                            && column.bytes().all(|byte| byte.is_ascii_digit())
                    })
            })
            .map_or((message, String::new()), |(base, position)| {
                (base, format!(" ({position}"))
            });
        Some(format!(
            "{base}, from buckets {} to {}, with a max diff of {}, over {} samples from {start} to {end}{position}",
            general_float(f64::from_bits(self.min_bucket_bits)),
            general_float(f64::from_bits(self.max_bucket_bits)),
            two_significant_digits(f64::from_bits(self.max_diff_bits)),
            self.count
        ))
    }
}

fn general_float(value: f64) -> String {
    if !value.is_finite() || value == 0.0 || (1e-4..1e6).contains(&value.abs()) {
        return crate::http_api::format_sample_value(value);
    }
    let text = format!("{value:e}");
    let (mantissa, exponent) = text.split_once('e').expect("scientific exponent");
    let exponent: i32 = exponent.parse().expect("integer exponent");
    format!("{mantissa}e{exponent:+03}")
}

fn two_significant_digits(value: f64) -> String {
    if !value.is_finite() || value == 0.0 {
        return general_float(value);
    }
    let text = format!("{value:.1e}");
    let (mantissa, exponent) = text.split_once('e').expect("scientific exponent");
    let exponent: i32 = exponent.parse().expect("integer exponent");
    if !(-4..2).contains(&exponent) {
        return format!(
            "{}e{exponent:+03}",
            mantissa.trim_end_matches('0').trim_end_matches('.')
        );
    }
    let precision = usize::try_from((1 - exponent).max(0)).expect("nonnegative precision");
    let text = format!("{value:.precision$}");
    if precision == 0 {
        text
    } else {
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn repair_details_merge_bounds_differences_sample_counts_and_times_across_serialization() {
        let mut first = HistogramQuantileRepair::new(1.0, 2.0, 1.23, 748_323_000);
        let second = HistogramQuantileRepair::new(0.5, f64::INFINITY, 10.0, 748_324_000);
        let second: HistogramQuantileRepair =
            serde_json::from_str(&serde_json::to_string(&second).unwrap()).unwrap();
        first.merge(&second);
        assert!(
            first.render("base for metric name \"h\" (1:25)").unwrap()
                == "base for metric name \"h\", from buckets 0.5 to +Inf, with a max diff of 10, over 2 samples from 1970-01-09T15:52:03Z to 1970-01-09T15:52:04Z (1:25)"
        );
        for (value, expected) in [
            (1.23, "1.2"),
            (10.0, "10"),
            (123.0, "1.2e+02"),
            (0.00123, "0.0012"),
            (0.000_099_9, "0.0001"),
        ] {
            assert!(two_significant_digits(value) == expected);
        }
    }
}
