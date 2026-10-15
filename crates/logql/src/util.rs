use std::fmt;

use krabka_units::{ByteSize, convert::ByteSizeExt};

// Parse Prometheus Duration Literal
/// Parses a Prometheus duration literal such as `1h30m` into nanoseconds.
///
/// This follows Prometheus `model.ParseDuration`: one or more `<amount><unit>`
/// chunks, units strictly from larger to smaller, and no sign, fraction or
/// whitespace. Returns `None` for an empty, malformed, out-of-order or
/// overflowing literal, so each caller keeps its own upstream error message.
#[must_use]
pub fn parse_prometheus_duration_literal(value: &str) -> Option<i64> {
    let mut rest = value;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut total_ns = 0_i128;

    while !rest.is_empty() {
        let amount_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        if amount_len == 0 {
            return None;
        }
        let amount = rest.get(..amount_len)?.parse::<i128>().ok()?;
        rest = rest.get(amount_len..)?;

        let unit_len = rest.bytes().take_while(u8::is_ascii_alphabetic).count();
        let unit = prometheus_duration_unit(rest.get(..unit_len)?)?;
        rest = rest.get(unit_len..)?;
        if previous_unit_order.is_some_and(|previous| unit.rank <= previous) {
            return None;
        }

        let chunk_ns = amount.checked_mul(unit.nanoseconds)?;
        total_ns = total_ns.checked_add(chunk_ns)?;
        previous_unit_order = Some(unit.rank);
        parsed_chunk = true;
    }

    if !parsed_chunk {
        return None;
    }
    i64::try_from(total_ns).ok()
}

// Prometheus Duration Unit
/// Looks up one Prometheus duration unit.
///
/// Returns the unit's rank from largest (`y`, 0) to smallest (`ns`, 8), a bit
/// that is unique to the unit, and the unit's length in nanoseconds. A year is
/// 365 days, as in Prometheus `model.ParseDuration`. Units are case-sensitive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PrometheusDurationUnit {
    pub rank: u8,
    pub bit: u16,
    pub nanoseconds: i128,
}

#[must_use]
pub fn prometheus_duration_unit(unit: &str) -> Option<PrometheusDurationUnit> {
    match unit {
        "y" => Some(PrometheusDurationUnit {
            rank: 0,
            bit: 0x001,
            nanoseconds: 31_536_000_000_000_000,
        }),
        "w" => Some(PrometheusDurationUnit {
            rank: 1,
            bit: 0x002,
            nanoseconds: 604_800_000_000_000,
        }),
        "d" => Some(PrometheusDurationUnit {
            rank: 2,
            bit: 0x004,
            nanoseconds: 86_400_000_000_000,
        }),
        "h" => Some(PrometheusDurationUnit {
            rank: 3,
            bit: 0x008,
            nanoseconds: 3_600_000_000_000,
        }),
        "m" => Some(PrometheusDurationUnit {
            rank: 4,
            bit: 0x010,
            nanoseconds: 60_000_000_000,
        }),
        "s" => Some(PrometheusDurationUnit {
            rank: 5,
            bit: 0x020,
            nanoseconds: 1_000_000_000,
        }),
        "ms" => Some(PrometheusDurationUnit {
            rank: 6,
            bit: 0x040,
            nanoseconds: 1_000_000,
        }),
        "us" => Some(PrometheusDurationUnit {
            rank: 7,
            bit: 0x080,
            nanoseconds: 1_000,
        }),
        "ns" => Some(PrometheusDurationUnit {
            rank: 8,
            bit: 0x100,
            nanoseconds: 1,
        }),
        _ => None,
    }
}

// Quoted Char
pub(crate) struct QuotedChar(pub(crate) char);

impl fmt::Display for QuotedChar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "'{}'", self.0)
    }
}

// Read Quoted Body
/// Why [`read_quoted_body`] stopped before the closing quote.
pub(crate) enum QuotedBodyError {
    /// The input ended right after a backslash.
    DanglingEscape,
    /// The input ended before the closing quote.
    Unterminated,
}

/// Decodes a double-quoted body whose first character is at `*pos`, just
/// after the opening quote, and advances `*pos` past the closing quote.
pub(crate) fn read_quoted_body(input: &str, pos: &mut usize) -> Result<String, QuotedBodyError> {
    let peek = |pos: usize| input[pos..].chars().next();
    let mut out = String::new();
    while let Some(ch) = peek(*pos) {
        *pos = pos.saturating_add(ch.len_utf8());
        match ch {
            '"' => return Ok(out),
            '\\' => {
                let Some(escaped) = peek(*pos) else {
                    return Err(QuotedBodyError::DanglingEscape);
                };
                *pos = pos.saturating_add(escaped.len_utf8());
                out.push(decode_quoted_escape(escaped));
            }
            _ => out.push(ch),
        }
    }
    Err(QuotedBodyError::Unterminated)
}

pub(crate) fn decode_quoted_escape(escaped: char) -> char {
    match escaped {
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        '"' => '"',
        '\\' => '\\',
        other => other,
    }
}

pub(crate) fn gcd_u64(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

pub(crate) fn is_ident_char(ch: char) -> bool {
    is_ident_start(ch) || ch.is_ascii_digit()
}

pub(crate) fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch == ':' || ch == '.' || ch.is_ascii_alphabetic()
}

/// The size units that the `LogQL` grammar itself admits.
///
/// The table is here and not in `krabka_units::parse::byte_size` because Loki
/// matches these units case-sensitively. Loki accepts `KiB`, `kB`, `KB`, and
/// `MB`, and it rejects `kib` and `mb`. The shared parser is case-insensitive,
/// and its use would widen the query language that this crate is a compatible
/// front-end for.
pub(crate) fn bytes_unit_multiplier(unit: &str) -> Option<f64> {
    match unit {
        "" | "B" => Some(1.0),
        "kB" | "KB" => Some(1_000.0),
        "MB" => Some(1_000_000.0),
        "GB" => Some(1_000_000_000.0),
        "TB" => Some(1_000_000_000_000.0),
        "KiB" => Some(1024.0),
        "MiB" => Some(1_048_576.0),
        "GiB" => Some(1_073_741_824.0),
        "TiB" => Some(1_099_511_627_776.0),
        _ => None,
    }
}

pub(crate) fn format_decimal_ratio(numerator: u128, denominator: u128) -> String {
    let whole = numerator / denominator;
    let mut remainder = numerator % denominator;
    if remainder == 0 {
        return whole.to_string();
    }

    let mut decimals = String::new();
    for _ in 0..9 {
        if remainder == 0 {
            break;
        }
        remainder *= 10;
        let digit = u8::try_from(remainder / denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{whole}.{decimals}")
}

pub(crate) fn parse_bytes_literal(value: &str) -> Option<ByteSize> {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let amount = value[..unit_start].parse::<f64>().ok()?;
    if !amount.is_finite() || amount < 0.0 {
        return None;
    }
    let multiplier = bytes_unit_multiplier(&value[unit_start..])?;
    Some(ByteSize::from_bytes_f64(amount * multiplier))
}

/// Returns the value of one ASCII hex digit, in either case.
///
/// Returns `None` for any byte outside `0-9`, `a-f` and `A-F`. This is the
/// digit check behind Go's `url.QueryUnescape`, which both Prometheus
/// `urlquery` templates and Loki form decoding rely on.
#[must_use]
pub const fn hex_digit_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use krabka_units::{ByteSize, convert::ByteSizeExt};

    use super::{
        PrometheusDurationUnit, QuotedChar, format_decimal_ratio, is_ident_start,
        parse_bytes_literal, parse_prometheus_duration_literal, prometheus_duration_unit,
    };

    struct DurationUnitCase {
        unit: &'static str,
        expected: PrometheusDurationUnit,
    }

    struct ByteLiteralCase {
        literal: &'static str,
        expected: f64,
    }

    #[test]
    fn ident_start_accepts_logql_identifier_prefixes() {
        for ch in ['_', ':', '.', 'a', 'Z'] {
            assert!(is_ident_start(ch), "{ch:?} should start an identifier");
        }
        assert!(!is_ident_start('0'));
        assert!(!is_ident_start('-'));
    }

    #[test]
    fn duration_units_cover_every_supported_unit() {
        let units = [
            DurationUnitCase {
                unit: "y",
                expected: PrometheusDurationUnit {
                    rank: 0,
                    bit: 0x001,
                    nanoseconds: 31_536_000_000_000_000,
                },
            },
            DurationUnitCase {
                unit: "w",
                expected: PrometheusDurationUnit {
                    rank: 1,
                    bit: 0x002,
                    nanoseconds: 604_800_000_000_000,
                },
            },
            DurationUnitCase {
                unit: "d",
                expected: PrometheusDurationUnit {
                    rank: 2,
                    bit: 0x004,
                    nanoseconds: 86_400_000_000_000,
                },
            },
            DurationUnitCase {
                unit: "h",
                expected: PrometheusDurationUnit {
                    rank: 3,
                    bit: 0x008,
                    nanoseconds: 3_600_000_000_000,
                },
            },
            DurationUnitCase {
                unit: "m",
                expected: PrometheusDurationUnit {
                    rank: 4,
                    bit: 0x010,
                    nanoseconds: 60_000_000_000,
                },
            },
            DurationUnitCase {
                unit: "s",
                expected: PrometheusDurationUnit {
                    rank: 5,
                    bit: 0x020,
                    nanoseconds: 1_000_000_000,
                },
            },
            DurationUnitCase {
                unit: "ms",
                expected: PrometheusDurationUnit {
                    rank: 6,
                    bit: 0x040,
                    nanoseconds: 1_000_000,
                },
            },
            DurationUnitCase {
                unit: "us",
                expected: PrometheusDurationUnit {
                    rank: 7,
                    bit: 0x080,
                    nanoseconds: 1_000,
                },
            },
            DurationUnitCase {
                unit: "ns",
                expected: PrometheusDurationUnit {
                    rank: 8,
                    bit: 0x100,
                    nanoseconds: 1,
                },
            },
        ];

        for case in units {
            assert_eq!(prometheus_duration_unit(case.unit), Some(case.expected));
        }
        assert_eq!(prometheus_duration_unit("fortnight"), None);
    }

    #[test]
    fn prometheus_duration_literals_parse_long_and_short_units() {
        assert_eq!(
            parse_prometheus_duration_literal("1y2w3d4h5m6s7ms8us9ns"),
            Some(
                31_536_000_000_000_000
                    + 2 * 604_800_000_000_000
                    + 3 * 86_400_000_000_000
                    + 4 * 3_600_000_000_000
                    + 5 * 60_000_000_000
                    + 6 * 1_000_000_000
                    + 7 * 1_000_000
                    + 8 * 1_000
                    + 9
            )
        );
        assert_eq!(parse_prometheus_duration_literal("1us"), Some(1_000));
        assert_eq!(parse_prometheus_duration_literal("1ns"), Some(1));
        assert_eq!(parse_prometheus_duration_literal("1m1h"), None);
        assert_eq!(parse_prometheus_duration_literal(""), None);
    }

    #[test]
    fn decimal_ratios_stop_at_nine_fractional_digits() {
        assert_eq!(format_decimal_ratio(1, 2), "0.5");
        assert_eq!(format_decimal_ratio(1, 3), "0.333333333");
        assert_eq!(format_decimal_ratio(1_234, 1_000), "1.234");
    }

    #[test]
    fn bytes_literals_cover_decimal_binary_and_invalid_amounts() {
        use assert2::{assert, check};

        for case in [
            ByteLiteralCase {
                literal: "0B",
                expected: 0.0,
            },
            ByteLiteralCase {
                literal: "2GB",
                expected: 2_000_000_000.0,
            },
            ByteLiteralCase {
                literal: "3TB",
                expected: 3_000_000_000_000.0,
            },
            ByteLiteralCase {
                literal: "4KiB",
                expected: 4_096.0,
            },
            ByteLiteralCase {
                literal: "5GiB",
                expected: 5_368_709_120.0,
            },
            ByteLiteralCase {
                literal: "6TiB",
                expected: 6_597_069_766_656.0,
            },
        ] {
            check!(
                parse_bytes_literal(case.literal) == Some(ByteSize::from_bytes_f64(case.expected))
            );
        }
        assert!(parse_bytes_literal("-1B").is_none());
    }

    #[test]
    fn quoted_char_display_wraps_character() {
        assert_eq!(QuotedChar('"').to_string(), "'\"'");
        assert_eq!(QuotedChar('x').to_string(), "'x'");
    }
}
