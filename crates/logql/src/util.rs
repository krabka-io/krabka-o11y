use std::fmt;

use krabka_units::{ByteSize, convert::ByteSizeExt};

#[cfg(test)]
mod tests {
    use krabka_units::{ByteSize, convert::ByteSizeExt};

    use super::{
        QuotedChar, duration_unit, format_decimal_ratio, is_ident_start, parse_bytes_literal,
        parse_prometheus_duration_literal,
    };

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
            ("y", (0, 0x001, 31_536_000_000_000_000)),
            ("w", (1, 0x002, 604_800_000_000_000)),
            ("d", (2, 0x004, 86_400_000_000_000)),
            ("h", (3, 0x008, 3_600_000_000_000)),
            ("m", (4, 0x010, 60_000_000_000)),
            ("s", (5, 0x020, 1_000_000_000)),
            ("ms", (6, 0x040, 1_000_000)),
            ("us", (7, 0x080, 1_000)),
            ("ns", (8, 0x100, 1)),
        ];

        for (unit, expected) in units {
            assert_eq!(duration_unit(unit), Some(expected), "unit {unit}");
        }
        assert_eq!(duration_unit("fortnight"), None);
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

        for (literal, expected) in [
            ("0B", 0.0),
            ("2GB", 2_000_000_000.0),
            ("3TB", 3_000_000_000_000.0),
            ("4KiB", 4_096.0),
            ("5GiB", 5_368_709_120.0),
            ("6TiB", 6_597_069_766_656.0),
        ] {
            check!(
                parse_bytes_literal(literal) == Some(ByteSize::from_bytes_f64(expected)),
                "{literal}"
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

// === split-modules: generated submodules ===
mod bytes_unit_multiplier;
mod decode_quoted_escape;
mod duration_unit;
mod format_decimal_ratio;
mod gcd_u64;
mod is_ident_char;
mod is_ident_start;
mod parse_bytes_literal;
mod parse_prometheus_duration_literal;
mod quoted_char;

use bytes_unit_multiplier::bytes_unit_multiplier;
pub(crate) use decode_quoted_escape::decode_quoted_escape;
pub(crate) use duration_unit::duration_unit;
pub(crate) use format_decimal_ratio::format_decimal_ratio;
pub(crate) use gcd_u64::gcd_u64;
pub(crate) use is_ident_char::is_ident_char;
pub(crate) use is_ident_start::is_ident_start;
pub(crate) use parse_bytes_literal::parse_bytes_literal;
pub(crate) use parse_prometheus_duration_literal::parse_prometheus_duration_literal;
pub(crate) use quoted_char::QuotedChar;
