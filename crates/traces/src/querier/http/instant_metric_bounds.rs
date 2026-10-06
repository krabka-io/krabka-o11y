use std::{
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{array::timezone::Tz, compute::kernels::cast_utils::string_to_datetime};
use num_traits::ToPrimitive as _;
use regex::Regex;

use super::{Uri, query_param};

const HOUR_NS: i64 = 3_600_000_000_000;

pub(crate) fn instant_metric_bounds(uri: &Uri) -> Result<(i64, i64, i64, i64), String> {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_nanos()).ok())
        .ok_or_else(|| "current timestamp out of range".to_string())?;
    instant_metric_bounds_at(uri, now_ns)
}

// Tempo pkg/api/http.go ParseQueryInstantRequest/determineBounds defaults to
// one hour ending now, independently defaults either bound, and ignores time.
fn instant_metric_bounds_at(uri: &Uri, now_ns: i64) -> Result<(i64, i64, i64, i64), String> {
    let since = query_param(uri, "since").filter(|value| !value.is_empty());
    let since_ns = since.as_deref().map_or(Ok(HOUR_NS), parse_since)?;
    let end_ns = timestamp_param(uri, "end", now_ns)?;
    let default_start = end_ns
        .min(now_ns)
        .checked_sub(since_ns)
        .ok_or_else(|| "start timestamp out of range".to_string())?;
    let start_ns = timestamp_param(uri, "start", default_start)?;
    let step_ns = end_ns
        .checked_sub(start_ns)
        .filter(|step| *step > 0)
        .ok_or_else(|| "end must be > start".to_string())?;
    Ok((start_ns, end_ns, step_ns, end_ns))
}

fn timestamp_param(uri: &Uri, key: &str, default: i64) -> Result<i64, String> {
    query_param(uri, key)
        .filter(|value| !value.is_empty())
        .map_or(Ok(default), |value| {
            parse_timestamp(&value).ok_or_else(|| {
                format!("could not parse '{key}' parameter: invalid timestamp {value:?}")
            })
        })
}

fn parse_timestamp(value: &str) -> Option<i64> {
    static RFC3339: OnceLock<Regex> = OnceLock::new();
    // Tempo rounds fractional epoch seconds to milliseconds before time.Unix.
    if value.contains('.')
        && let Some(seconds) = krabka_logql::parse_go_float(value)
        && seconds.is_finite()
    {
        let whole = seconds.trunc().to_i64().unwrap_or(i64::MIN);
        let nanos = ((seconds.fract() * 1_000.0).round() / 1_000.0 * 1_000_000_000.0)
            .to_i64()
            .unwrap_or(i64::MIN);
        return Some(whole.wrapping_mul(1_000_000_000).wrapping_add(nanos));
    }
    if let Ok(integer) = value.parse::<i64>() {
        return Some(if value.len() <= 10 {
            integer.wrapping_mul(1_000_000_000)
        } else {
            integer
        });
    }
    // Arrow supplies the calendar parser; restrict its broader ISO8601 grammar
    // to the RFC3339Nano form accepted by Go time.Parse.
    let grammar = RFC3339.get_or_init(|| {
        Regex::new(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{1,2}:[0-9]{2}:[0-5][0-9](?:[.,][0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$")
            .expect("fixed RFC3339 grammar")
    });
    if !grammar.is_match(value) {
        return None;
    }
    let (calendar, offset_seconds) = if let Some(calendar) = value.strip_suffix('Z') {
        (calendar, 0_i64)
    } else {
        let split = value.len().checked_sub(6)?;
        let (calendar, zone) = value.split_at(split);
        let hour = zone[1..3].parse::<i64>().ok()?;
        let minute = zone[4..6].parse::<i64>().ok()?;
        // Go's fallback RFC3339 parser accepts 24 hours and 60 minutes.
        if hour > 24 || minute > 60 {
            return None;
        }
        let sign = if zone.starts_with('-') { -1 } else { 1 };
        (calendar, sign * (hour * 3_600 + minute * 60))
    };
    let mut calendar = calendar.replace(',', ".");
    // Go time.Parse falls back to its layout parser, which permits one digit
    // for the hour; Arrow's fixed-position calendar parser requires two.
    if calendar.as_bytes().get(12) == Some(&b':') {
        calendar.insert(11, '0');
    }
    let utc = "+00:00".parse::<Tz>().ok()?;
    let parsed = string_to_datetime(&utc, &format!("{calendar}Z")).ok()?;
    Some(
        parsed
            .timestamp()
            .wrapping_sub(offset_seconds)
            .wrapping_mul(1_000_000_000)
            .wrapping_add(i64::from(parsed.timestamp_subsec_nanos())),
    )
}

fn parse_since(value: &str) -> Result<i64, String> {
    let invalid =
        || format!("could not parse 'since' parameter: not a valid duration string: {value:?}");
    if value == "0" {
        return Ok(0);
    }
    let mut rest = value;
    let mut previous = None;
    let mut total = 0_i64;
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return Err(invalid());
        }
        let amount = rest[..digits].parse::<u64>().map_err(|_| invalid())?;
        rest = &rest[digits..];
        let unit_end = rest
            .bytes()
            .take_while(|byte| !byte.is_ascii_digit())
            .count();
        let unit = &rest[..unit_end];
        let (order, multiplier) = match unit {
            "y" => (0, 31_536_000_000_000_000_i64),
            "w" => (1, 604_800_000_000_000),
            "d" => (2, 86_400_000_000_000),
            "h" => (3, HOUR_NS),
            "m" => (4, 60_000_000_000),
            "s" => (5, 1_000_000_000),
            "ms" => (6, 1_000_000),
            "" => return Err(invalid()),
            _ => {
                return Err(format!(
                    "could not parse 'since' parameter: unknown unit {unit:?} in duration {value:?}"
                ));
            }
        };
        if previous.is_some_and(|prior| order <= prior) {
            return Err(invalid());
        }
        total = i64::try_from(amount)
            .ok()
            .and_then(|amount| amount.checked_mul(multiplier))
            .and_then(|chunk| total.checked_add(chunk))
            .ok_or_else(|| {
                "could not parse 'since' parameter: duration out of range".to_string()
            })?;
        previous = Some(order);
        rest = &rest[unit_end..];
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn at(query: &str) -> Result<(i64, i64, i64, i64), String> {
        instant_metric_bounds_at(
            &format!("/api/metrics/query?{query}").parse().unwrap(),
            7_200_000_000_000,
        )
    }

    #[test]
    fn defaults_ignore_time_and_independently_fill_bounds() {
        let default = (
            3_600_000_000_000,
            7_200_000_000_000,
            3_600_000_000_000,
            7_200_000_000_000,
        );
        for query in ["", "time=0", "time=bogus", "start=&end=&since="] {
            assert!(at(query) == Ok(default));
        }
        assert!(
            at("end=6000")
                == Ok((
                    2_400_000_000_000,
                    6_000_000_000_000,
                    3_600_000_000_000,
                    6_000_000_000_000
                ))
        );
        assert!(
            at("end=8000&since=30m")
                == Ok((
                    5_400_000_000_000,
                    8_000_000_000_000,
                    2_600_000_000_000,
                    8_000_000_000_000
                ))
        );
        assert!(
            at("start=7000&since=1m")
                == Ok((
                    7_000_000_000_000,
                    7_200_000_000_000,
                    200_000_000_000,
                    7_200_000_000_000
                ))
        );
        assert!(at("start=6000&end=6100&since=bogus").is_err());
        assert!(at("start=7201").is_err());
        assert!(at("since=0").is_err());
        assert!(at("start=0&end=9223372036854775807") == Ok((0, i64::MAX, i64::MAX, i64::MAX)));
        assert!(at("start=-10000000000&end=9223372036854775807").is_err());
    }

    #[test]
    fn source_timestamp_forms_keep_integer_precision_and_fractional_rounding() {
        for (input, expected) in [
            ("12", 12_000_000_000),
            ("0x1.8p1", 3_000_000_000),
            ("0x_1.8p1", 3_000_000_000),
            ("-0x1.8p1", -3_000_000_000),
            ("1_2.5", 12_500_000_000),
            ("1.5e0_1", 15_000_000_000),
            ("10000000001", 10_000_000_001),
            ("1700000000000000001", 1_700_000_000_000_000_001),
            ("12.12349", 12_123_000_000),
            ("12.1235", 12_123_000_000),
            ("12.12351", 12_124_000_000),
            ("-12.1235", -12_123_000_000),
            ("1970-01-01T00:00:01.000000001Z", 1_000_000_001),
            ("1970-01-01T0:00:01Z", 1_000_000_000),
            ("1970-01-01T1:00:01.5+01:00", 1_500_000_000),
            ("1970-01-01T01:00:01+01:00", 1_000_000_000),
            ("1970-01-01T00:00:01+24:60", -89_999_000_000_000),
            ("1970-01-01T00:00:01,1234567891Z", 1_123_456_789),
            (
                "0000-01-01T00:00:00Z",
                (-62_167_219_200_i64).wrapping_mul(1_000_000_000),
            ),
        ] {
            assert!(parse_timestamp(input) == Some(expected), "{input}");
        }
        for input in [
            "bogus",
            "0x1p1",
            "1_000",
            "1_.5",
            "1._5",
            "0x1._8p1",
            "0x1.fffffffffffff8p1023",
            "1.e309",
            "1970-01-01",
            "1970-01-01 00:00:01Z",
            "1970-01-01T00:00:60Z",
            "١٩٧٠-01-01T00:00:01Z",
            "1970-01-01T00:00:01+١٢:٠٠",
        ] {
            assert!(parse_timestamp(input).is_none(), "{input}");
        }
    }

    #[test]
    fn since_uses_prometheus_units_and_rejects_go_only_units() {
        assert!(parse_since("1w2d3h4m5s6ms") == Ok(788_645_006_000_000));
        for value in [
            "-1s",
            "1.5h",
            "1us",
            "1ns",
            "1m1h",
            "1h1h",
            "9999999999999999999y",
        ] {
            assert!(parse_since(value).is_err(), "{value}");
        }
    }
}
