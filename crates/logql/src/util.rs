use std::fmt;

use krabka_units::{ByteSize, convert::ByteSizeExt};

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

mod bytes_unit_multiplier;
mod decode_quoted_escape;
mod format_decimal_ratio;
mod gcd_u64;
mod hex_digit_value;
mod is_ident_char;
mod is_ident_start;
mod parse_bytes_literal;
mod parse_prometheus_duration_literal;
mod prometheus_duration_unit;
mod quoted_char;
mod read_quoted_body;

use bytes_unit_multiplier::bytes_unit_multiplier;
pub(crate) use decode_quoted_escape::decode_quoted_escape;
pub(crate) use format_decimal_ratio::format_decimal_ratio;
pub(crate) use gcd_u64::gcd_u64;
pub use hex_digit_value::hex_digit_value;
pub(crate) use is_ident_char::is_ident_char;
pub(crate) use is_ident_start::is_ident_start;
pub(crate) use parse_bytes_literal::parse_bytes_literal;
pub use parse_prometheus_duration_literal::parse_prometheus_duration_literal;
pub use prometheus_duration_unit::{PrometheusDurationUnit, prometheus_duration_unit};
pub(crate) use quoted_char::QuotedChar;
pub(crate) use read_quoted_body::{QuotedBodyError, read_quoted_body};
