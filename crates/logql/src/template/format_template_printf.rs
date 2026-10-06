//! Go fmt formatting for the value kinds reachable from Loki templates.
//!
//! Parsing and diagnostics follow Go 1.26.5 `src/fmt/print.go::doPrintf`;
//! scalar formatting follows `src/fmt/format.go`. Preserve types before formatting.
//! Primary-source SHA256: print.go 57250462a0d06d61f124b4530493bcdd150e9de29600bd60347e1c74457ac8a1;
//! format.go a668c0d6bb23f6ac929b73ef661a65be2225a9f665052a2ce02c4c58339c3335.
use super::TemplateRuntimeValue;
mod floating;
mod printable;
pub(in crate::template) mod raw;
mod scalar;

#[derive(Clone, Copy, Default)]
struct Spec {
    flags: [bool; 5],
    sharp_v: bool,
    plus_v: bool,
    width: Option<usize>,
    precision: Option<usize>,
}

impl Spec {
    const SHARP: usize = 0;
    const PLUS: usize = 1;
    const MINUS: usize = 2;
    const ZERO: usize = 3;
    const SPACE: usize = 4;

    fn with_flag(mut self, flag: usize, value: bool) -> Self {
        self.flags[flag] = value;
        self
    }

    fn pad(self, value: &str) -> String {
        if !self.flags[Spec::ZERO] {
            return super::format_template_printf_string(
                value,
                self.width,
                None,
                self.flags[Spec::MINUS],
            );
        }
        let count = self
            .width
            .unwrap_or(0)
            .saturating_sub(value.chars().count());
        if count == 0 {
            return value.to_owned();
        }
        if self.flags[Spec::MINUS] {
            format!("{value}{}", " ".repeat(count))
        } else {
            format!(
                "{}{value}",
                if self.flags[Spec::ZERO] { "0" } else { " " }.repeat(count)
            )
        }
    }
}

/// Quote a Go byte string without replacing malformed UTF-8 sequences.
///
/// # Panics
/// Panics if the quoted byte string violates the formatter's UTF-8 invariant.
#[must_use]
pub fn quote_go_bytes(value: &[u8]) -> String {
    String::from_utf8(raw::string(value, 'q', Spec::default()))
        .expect("Go quoted strings are UTF-8")
}

pub(crate) fn format_template_printf(args: &[String]) -> String {
    format_template_printf_values(
        &args
            .iter()
            .cloned()
            .map(TemplateRuntimeValue::String)
            .collect::<Vec<_>>(),
    )
}

pub(crate) fn format_template_printf_values(args: &[TemplateRuntimeValue]) -> String {
    super::template_bytes_to_string(&format_template_printf_bytes(args))
}

pub(crate) fn format_template_printf_bytes(args: &[TemplateRuntimeValue]) -> Vec<u8> {
    let Some(format) = args.first() else {
        return Vec::new();
    };
    let format = format.rendered_bytes();
    let units = raw::runes(&format);
    let chars: Vec<_> = units.iter().map(|unit| unit.0).collect();
    let values = &args[1..];
    let mut cursor = 0;
    let mut arg = 0;
    let mut reordered = false;
    let mut output = Vec::new();
    while cursor < chars.len() {
        if chars[cursor] != '%' {
            output.extend_from_slice(&format[units[cursor].1.clone()]);
            cursor += 1;
            continue;
        }
        cursor += 1;
        let mut spec = Spec::default();
        while let Some(flag) = chars.get(cursor) {
            match flag {
                '#' => spec.flags[Spec::SHARP] = true,
                '+' => spec.flags[Spec::PLUS] = true,
                '-' => spec.flags[Spec::MINUS] = true,
                '0' => spec.flags[Spec::ZERO] = true,
                ' ' => spec.flags[Spec::SPACE] = true,
                _ => break,
            }
            cursor += 1;
        }
        let mut good = true;
        let mut indexed = index(
            &chars,
            &mut cursor,
            &mut arg,
            values.len(),
            &mut reordered,
            &mut good,
        );
        if chars.get(cursor) == Some(&'*') {
            cursor += 1;
            match star(values, &mut arg) {
                Some(width) => {
                    spec.width = Some(
                        usize::try_from(width.unsigned_abs())
                            .expect("validated formatter width fits usize"),
                    );
                    if width < 0 {
                        spec.flags[Spec::MINUS] = true;
                        spec.flags[Spec::ZERO] = false;
                    }
                }
                None => output.extend_from_slice(b"%!(BADWIDTH)"),
            }
            indexed = false;
        } else {
            spec.width = number(&chars, &mut cursor);
            if indexed && spec.width.is_some() {
                good = false;
            }
        }
        if chars.get(cursor) == Some(&'.') && cursor + 1 < chars.len() {
            cursor += 1;
            if indexed {
                good = false;
            }
            indexed = index(
                &chars,
                &mut cursor,
                &mut arg,
                values.len(),
                &mut reordered,
                &mut good,
            );
            if chars.get(cursor) == Some(&'*') {
                cursor += 1;
                match star(values, &mut arg) {
                    Some(precision) if precision >= 0 => {
                        spec.precision = Some(
                            usize::try_from(precision)
                                .expect("validated formatter precision fits usize"),
                        );
                    }
                    _ => output.extend_from_slice(b"%!(BADPREC)"),
                }
                indexed = false;
            } else {
                spec.precision = Some(number(&chars, &mut cursor).unwrap_or(0));
            }
        }
        if !indexed {
            index(
                &chars,
                &mut cursor,
                &mut arg,
                values.len(),
                &mut reordered,
                &mut good,
            );
        }
        let Some(&verb) = chars.get(cursor) else {
            output.extend_from_slice(b"%!(NOVERB)");
            break;
        };
        cursor += 1;
        if verb == '%' {
            output.push(b'%');
        } else if !good {
            output.extend_from_slice(format!("%!{verb}(BADINDEX)").as_bytes());
        } else if let Some(value) = values.get(arg) {
            if verb == 'v' {
                spec.sharp_v = spec.flags[Spec::SHARP];
                spec.plus_v = spec.flags[Spec::PLUS];
                spec.flags[Spec::SHARP] = false;
                spec.flags[Spec::PLUS] = false;
            }
            output.extend_from_slice(&raw::render(value, verb, spec, false));
            arg += 1;
        } else {
            output.extend_from_slice(format!("%!{verb}(MISSING)").as_bytes());
        }
    }
    if !reordered && arg < values.len() {
        output.extend_from_slice(b"%!(EXTRA ");
        for (i, value) in values[arg..].iter().enumerate() {
            if i > 0 {
                output.extend_from_slice(b", ");
            }
            if matches!(value, TemplateRuntimeValue::Json(serde_json::Value::Null)) {
                output.extend_from_slice(b"<nil>");
            } else {
                output.extend_from_slice(scalar::type_name(value).as_bytes());
                output.push(b'=');
                output.extend_from_slice(&raw::render(value, 'v', Spec::default(), false));
            }
        }
        output.push(b')');
    }
    output
}

fn number(chars: &[char], cursor: &mut usize) -> Option<usize> {
    let mut value = None;
    while let Some(digit) = chars
        .get(*cursor)
        .and_then(|ch| ch.to_digit(10).filter(|_| ch.is_ascii()))
    {
        let previous = value.unwrap_or(0);
        if previous > 1_000_000 {
            *cursor = chars.len();
            return None;
        }
        value = Some(previous * 10 + digit as usize);
        *cursor += 1;
    }
    value
}

fn index(
    chars: &[char],
    cursor: &mut usize,
    arg: &mut usize,
    count: usize,
    reordered: &mut bool,
    good: &mut bool,
) -> bool {
    if chars.get(*cursor) != Some(&'[') {
        return false;
    }
    *reordered = true;
    let start = *cursor;
    *cursor += 1;
    let closing = chars[*cursor..]
        .iter()
        .position(|ch| *ch == ']')
        .map(|offset| *cursor + offset);
    let number = if let Some(closing) = closing {
        let value = number(&chars[..closing], cursor);
        let complete = *cursor == closing;
        *cursor = closing + 1;
        value.filter(|_| complete)
    } else {
        *cursor = start + 1;
        None
    };

    match number {
        Some(index) if index > 0 && index <= count => {
            *arg = index - 1;
            true
        }
        Some(_) => {
            *good = false;
            true
        }
        None => {
            *good = false;
            false
        }
    }
}

fn star(values: &[TemplateRuntimeValue], arg: &mut usize) -> Option<i64> {
    let value = values.get(*arg)?;
    *arg += 1;
    let integer = match value {
        TemplateRuntimeValue::Integer(value) | TemplateRuntimeValue::Integer64(value) => *value,
        TemplateRuntimeValue::Byte(value) => i64::from(*value),
        _ => return None,
    };
    (integer.unsigned_abs() <= 1_000_000).then_some(integer)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    fn printf(format: &str, values: &[TemplateRuntimeValue]) -> String {
        let mut args = vec![TemplateRuntimeValue::String(format.into())];
        args.extend_from_slice(values);
        format_template_printf_values(&args)
    }
    #[test]
    fn go_byte_strings_preserve_invalid_sequences_in_formats_values_and_compositions() {
        let bytes = vec![0xff, 0xc3, b'(', 0xe2, 0x82, 0xac];
        let value = TemplateRuntimeValue::Bytes(bytes.clone());
        let format = |format: &str| {
            format_template_printf_bytes(&[
                TemplateRuntimeValue::String(format.into()),
                value.clone(),
            ])
        };
        check!(format("%s") == bytes);
        check!(format("%.2s") == vec![0xff, 0xc3]);
        check!(format("%q") == "\"\\xff\\xc3(€\"".as_bytes());
        check!(format("%+q") == b"\"\\xff\\xc3(\\u20ac\"");
        check!(format("%#q") == "\"\\xff\\xc3(€\"".as_bytes());
        check!(format("%.2x") == b"ffc3");
        check!(format("%8.2s") == vec![b' ', b' ', b' ', b' ', b' ', b' ', 0xff, 0xc3]);
        check!(format("%d") == [b"%!d(string=".as_slice(), &bytes, b")"].concat());
        check!(
            format_template_printf_bytes(&[
                TemplateRuntimeValue::Bytes(vec![0xff, b'%', b's']),
                value.clone()
            ]) == [vec![0xff], bytes.clone()].concat()
        );
        let object = TemplateRuntimeValue::Object(std::collections::BTreeMap::from([
            ("raw".into(), value),
            ("n".into(), TemplateRuntimeValue::Integer64(7)),
            (
                "nil".into(),
                TemplateRuntimeValue::Json(serde_json::Value::Null),
            ),
            (
                "time".into(),
                TemplateRuntimeValue::Time(super::super::TemplateTime::zero()),
            ),
        ]));
        check!(
            printf("%#v", std::slice::from_ref(&object))
                == "map[string]interface {}{\"n\":7, \"nil\":interface {}(nil), \"raw\":\"\\xff\\xc3(€\", \"time\":time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)}"
        );
        check!(
            printf("%q", &[object])
                == "map[\"n\":'\\a' \"nil\":<nil> \"raw\":\"\\xff\\xc3(€\" \"time\":\"0001-01-01 00:00:00 +0000 UTC\"]"
        );
        check!(
            printf(
                "%#v",
                &[TemplateRuntimeValue::Array(vec![
                    TemplateRuntimeValue::Integer(2),
                    TemplateRuntimeValue::Bytes(vec![0xff]),
                    TemplateRuntimeValue::Json(serde_json::Value::Null)
                ])]
            ) == "[]interface {}{2, \"\\xff\", interface {}(nil)}"
        );
    }
    #[test]
    fn go_time_stringer_go_string_and_reflected_fields_keep_their_types() {
        let time = super::super::TemplateTime::parse(
            "2006-01-02 15:04:05.000000000",
            "UTC",
            "2024-01-02 12:04:05.123456789",
        )
        .unwrap();
        let value = TemplateRuntimeValue::Time(time);
        for (format, expected) in [
            ("%T", "time.Time"),
            ("%s", "2024-01-02 12:04:05.123456789 +0000 UTC"),
            ("%.3v", "202"),
            (
                "%#v",
                "time.Date(2024, time.January, 2, 12, 4, 5, 123456789, time.UTC)",
            ),
            ("%d", "{123456789 63839793845 0}"),
            ("%p", "%!p(time.Time={123456789 63839793845 <nil>})"),
            ("%c", "{� � %!c(*time.Location=<nil>)}"),
            (
                "%f",
                "{%!f(uint64=123456789) %!f(int64=63839793845) %!f(*time.Location=<nil>)}",
            ),
        ] {
            check!(printf(format, std::slice::from_ref(&value)) == expected);
        }
    }
    #[test]
    fn go_argument_selection_and_error_diagnostics() {
        let values = [
            TemplateRuntimeValue::Integer(6),
            TemplateRuntimeValue::Integer(2),
            TemplateRuntimeValue::Float(1.25),
        ];
        check!(printf("%[3]*.[2]*[3]f", &values) == "%!(BADWIDTH)1.25");
        check!(printf("%[1]*.[2]*[3]f", &values) == "  1.25");
        check!(printf("%[3]f %f", &values) == "1.250000 %!f(MISSING)");
        check!(printf("%[9]d", &values) == "%!d(BADINDEX)");
        check!(printf("%[2]3d", &values) == "%!d(BADINDEX)");
        check!(printf("%d %d", &[TemplateRuntimeValue::Integer(3)]) == "3 %!d(MISSING)");
        check!(printf("%", &[]) == "%!(NOVERB)");
        check!(printf("%%", &[TemplateRuntimeValue::Integer(3)]) == "%%!(EXTRA int=3)");
        check!(
            printf(
                "%*d",
                &[
                    TemplateRuntimeValue::String("bad".into()),
                    TemplateRuntimeValue::Integer(3)
                ]
            ) == "%!(BADWIDTH)3"
        );
        check!(
            printf(
                "%.*f",
                &[
                    TemplateRuntimeValue::Integer(-1),
                    TemplateRuntimeValue::Float(1.25)
                ]
            ) == "%!(BADPREC)1.250000"
        );
        check!(printf("%f", &[TemplateRuntimeValue::String("1.25".into())]) == "%!f(string=1.25)");
    }
    #[test]
    fn typed_integer_bases_signs_precision_and_unicode() {
        check!(
            printf(
                "%+06d|%#08x|%#o|%O|%b|%.0d",
                &[
                    TemplateRuntimeValue::Integer(42),
                    TemplateRuntimeValue::Integer(42),
                    TemplateRuntimeValue::Integer(42),
                    TemplateRuntimeValue::Integer(42),
                    TemplateRuntimeValue::Integer(42),
                    TemplateRuntimeValue::Integer(0)
                ]
            ) == "+00042|0x0000002a|052|0o52|101010|"
        );
        check!(
            printf(
                "%c|%q|%#U",
                &[
                    TemplateRuntimeValue::Integer(955),
                    TemplateRuntimeValue::Integer(955),
                    TemplateRuntimeValue::Integer(955)
                ]
            ) == "λ|'λ'|U+03BB 'λ'"
        );
        check!(
            printf(
                "%T/%T/%T",
                &[
                    TemplateRuntimeValue::Integer(1),
                    TemplateRuntimeValue::Integer64(1),
                    TemplateRuntimeValue::Byte(1)
                ]
            ) == "int/int64/uint8"
        );
        check!(printf("%#v", &[TemplateRuntimeValue::Byte(42)]) == "0x2a");
        check!(printf("%d", &[TemplateRuntimeValue::Integer(i64::MIN)]) == "-9223372036854775808");
    }
    #[test]
    fn go_flags_and_diagnostics_compose_without_string_coercion() {
        let values = [
            TemplateRuntimeValue::Float(1.25),
            TemplateRuntimeValue::Float(1.25),
            TemplateRuntimeValue::Float(1.25),
            TemplateRuntimeValue::Float(1.25),
            TemplateRuntimeValue::Float(1.5),
            TemplateRuntimeValue::String("éλ".into()),
        ];
        check!(
            printf("%#g|%#x|%#X|%#.3x|%.0x|%+q", &values)
                == "1.25000|0x1.4000p+00|0X1.4P+00|0x1.400p+00|0x1p+01|\"\\u00e9\\u03bb\""
        );
        check!(
            printf(
                "%[x]d|%[]d|%[3.2]d|%[d",
                &[
                    TemplateRuntimeValue::Integer(1),
                    TemplateRuntimeValue::Integer(2),
                    TemplateRuntimeValue::Integer(3)
                ]
            ) == "%!d(BADINDEX)|%!d(BADINDEX)|%!d(BADINDEX)|%!d(BADINDEX)"
        );
        check!(
            printf(
                "%+08.3d|%#O|%05s|%#05v",
                &[
                    TemplateRuntimeValue::Integer(42),
                    TemplateRuntimeValue::Integer(42),
                    TemplateRuntimeValue::String("x".into()),
                    TemplateRuntimeValue::String("x".into())
                ]
            ) == "    +042|0o052|0000x|00\"x\""
        );
        check!(
            printf(
                "%#v/%T",
                &[
                    TemplateRuntimeValue::Labels(super::super::Labels::from([(
                        "a".into(),
                        "b".into()
                    )])),
                    TemplateRuntimeValue::Labels(super::super::Labels::new())
                ]
            ) == "map[string]string{\"a\":\"b\"}/map[string]string"
        );
        check!(
            printf(
                "%q/%#q",
                &[
                    TemplateRuntimeValue::String("\u{200b}".into()),
                    TemplateRuntimeValue::String("\u{200b}".into())
                ]
            ) == "\"\\u200b\"/`\u{200b}`"
        );
        check!(format_template_printf(&["%f".into(), "1.25".into()]) == "%!f(string=1.25)");
    }
    #[test]
    fn precision_flags_recur_into_collection_elements() {
        let values = [
            TemplateRuntimeValue::Json(serde_json::json!([true, 1.25, "abc"])),
            TemplateRuntimeValue::Json(serde_json::json!({"ab":"cd"})),
            TemplateRuntimeValue::Json(serde_json::json!([null])),
        ];
        check!(
            printf("%#08v|%.1v|%#v", &values)
                == "[]interface {}{0000true, 00001.25, 000\"abc\"}|map[a:c]|[]interface {}{interface {}(nil)}"
        );
        check!(
            printf(
                "%E|%F|%G|%t",
                &[
                    TemplateRuntimeValue::Float(1.25),
                    TemplateRuntimeValue::Float(1.25),
                    TemplateRuntimeValue::Float(1_000_000.0),
                    TemplateRuntimeValue::Json(serde_json::json!(true))
                ]
            ) == "1.250000E+00|1.250000|1E+06|true"
        );
    }
    #[test]
    fn nil_precision_differs_from_string_and_nested_interface() {
        check!(
            printf(
                "%.1T|%.1v|%#v|%#08v",
                &[
                    TemplateRuntimeValue::Json(serde_json::Value::Null),
                    TemplateRuntimeValue::Json(serde_json::Value::Null),
                    TemplateRuntimeValue::Json(serde_json::json!([null])),
                    TemplateRuntimeValue::Json(serde_json::json!([null]))
                ]
            ) == "<nil>|<nil>|[]interface {}{interface {}(nil)}|[]interface {}{interface {}(nil)}"
        );
    }
    #[test]
    fn strings_collections_and_float_formats_preserve_types() {
        check!(
            printf(
                "%.1s|%.1x|%+q|%#q",
                &[
                    TemplateRuntimeValue::String("λx".into()),
                    TemplateRuntimeValue::String("λx".into()),
                    TemplateRuntimeValue::String("λ".into()),
                    TemplateRuntimeValue::String("hello".into())
                ]
            ) == "λ|ce|\"\\u03bb\"|`hello`"
        );
        check!(
            printf(
                "%e|%.3g|%x|%b",
                &[
                    TemplateRuntimeValue::Float(1.25),
                    TemplateRuntimeValue::Float(12345.0),
                    TemplateRuntimeValue::Float(1.25),
                    TemplateRuntimeValue::Float(1.25)
                ]
            ) == "1.250000e+00|1.23e+04|0x1.4p+00|5629499534213120p-52"
        );
        check!(
            printf(
                "%#v",
                &[TemplateRuntimeValue::Json(
                    serde_json::json!({"a":true,"b":[1,"x"]})
                )]
            ) == "map[string]interface {}{\"a\":true, \"b\":[]interface {}{1, \"x\"}}"
        );
        check!(
            printf(
                "%d",
                &[TemplateRuntimeValue::Json(serde_json::json!([true, 2]))]
            ) == "[%!d(bool=true) %!d(float64=2)]"
        );
    }
}
