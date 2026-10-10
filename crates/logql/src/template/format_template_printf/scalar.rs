use std::fmt::Write as _;

use serde_json::Value;

use super::{super::TemplateRuntimeValue, Spec, floating, printable::printable};

pub(super) fn type_name(value: &TemplateRuntimeValue) -> &'static str {
    match value {
        TemplateRuntimeValue::Reference(value) => type_name(&value.resolve()),
        TemplateRuntimeValue::String(_)
        | TemplateRuntimeValue::Json(Value::String(_))
        | TemplateRuntimeValue::Bytes(_) => "string",
        TemplateRuntimeValue::Integer(_) => "int",
        TemplateRuntimeValue::Integer64(_) => "int64",
        TemplateRuntimeValue::Byte(_) => "uint8",
        TemplateRuntimeValue::Complex(_, _) => "complex128",
        TemplateRuntimeValue::Float(_) | TemplateRuntimeValue::Json(Value::Number(_)) => "float64",
        TemplateRuntimeValue::Json(Value::Bool(_)) => "bool",
        TemplateRuntimeValue::Json(Value::Null)
        | TemplateRuntimeValue::QueryError(_)
        | TemplateRuntimeValue::NilError => "<nil>",
        TemplateRuntimeValue::Json(Value::Array(_)) | TemplateRuntimeValue::Array(_) => {
            "[]interface {}"
        }
        TemplateRuntimeValue::Json(Value::Object(_)) | TemplateRuntimeValue::Object(_) => {
            "map[string]interface {}"
        }
        TemplateRuntimeValue::Labels(_) | TemplateRuntimeValue::ByteLabels(_) => {
            "map[string]string"
        }
        TemplateRuntimeValue::Time(_) => "time.Time",
        TemplateRuntimeValue::TimeLocation(_) => "*time.Location",
        TemplateRuntimeValue::Duration(_) => "time.Duration",
        TemplateRuntimeValue::Month(_) => "time.Month",
        TemplateRuntimeValue::Weekday(_) => "time.Weekday",
        TemplateRuntimeValue::ByteSlice(_) => "[]uint8",
        TemplateRuntimeValue::Integer32(_) => "int32",
        TemplateRuntimeValue::Unsigned32(_) => "uint32",
        TemplateRuntimeValue::CounterResetHint(_) => "histogram.CounterResetHint",
        TemplateRuntimeValue::FloatSlice(_) => "[]float64",
        TemplateRuntimeValue::HistogramSpans(_) => "[]histogram.Span",
        TemplateRuntimeValue::HistogramSpan(_) => "histogram.Span",
        TemplateRuntimeValue::FloatHistogram(_) => "*histogram.FloatHistogram",
        TemplateRuntimeValue::HistogramBucket(_) => "histogram.Bucket[float64]",
        TemplateRuntimeValue::HistogramIterator(value) => value.kind,
        TemplateRuntimeValue::HistogramError(error) => error.kind,
        TemplateRuntimeValue::TimePointer(_) => "*time.Time",
        TemplateRuntimeValue::DurationPointer(_) => "*time.Duration",
        TemplateRuntimeValue::SafeHtml(_) => "template.HTML",
        TemplateRuntimeValue::QueryResult(_) => "template.queryResult",
        TemplateRuntimeValue::Sample(_) => "*template.sample",
    }
}

pub(super) fn render(value: &TemplateRuntimeValue, verb: char, spec: Spec) -> String {
    if let TemplateRuntimeValue::Reference(value) = value {
        return render(&value.resolve(), verb, spec);
    }
    let invalid = || {
        format!(
            "%!{verb}({}={})",
            type_name(value),
            render(value, 'v', spec)
        )
    };
    // Go's fmt prints a `Stringer`'s text for these verbs, and its fields otherwise.
    let renders_as_text = matches!(verb, 's' | 'q' | 'x' | 'X') || (verb == 'v' && !spec.sharp_v);
    if verb == 'T' && matches!(value, TemplateRuntimeValue::Json(Value::Null)) {
        return spec.pad("<nil>");
    }
    if verb == 'T' {
        return spec.pad(&truncate(type_name(value), spec.precision));
    }
    match value {
        TemplateRuntimeValue::Reference(value) => render(&value.resolve(), verb, spec),
        TemplateRuntimeValue::String(value) | TemplateRuntimeValue::Json(Value::String(value)) => {
            string(value, verb, spec)
        }
        TemplateRuntimeValue::Integer(value) | TemplateRuntimeValue::Integer64(value) => {
            integer(value.cast_unsigned(), *value < 0, true, verb, spec).unwrap_or_else(invalid)
        }
        TemplateRuntimeValue::Integer32(value) => integer(
            i64::from(*value).cast_unsigned(),
            *value < 0,
            true,
            verb,
            spec,
        )
        .unwrap_or_else(invalid),
        TemplateRuntimeValue::Unsigned32(value) => {
            integer(u64::from(*value), false, false, verb, spec).unwrap_or_else(invalid)
        }
        TemplateRuntimeValue::CounterResetHint(value) | TemplateRuntimeValue::Byte(value) => {
            integer(u64::from(*value), false, false, verb, spec).unwrap_or_else(invalid)
        }
        TemplateRuntimeValue::FloatHistogram(value) if renders_as_text => {
            string(&value.as_string(), verb, spec)
        }
        TemplateRuntimeValue::FloatHistogram(value) => {
            reflection(&value.reflection(), verb, spec, 0)
        }
        TemplateRuntimeValue::HistogramBucket(value) if renders_as_text => {
            string(&value.as_string(), verb, spec)
        }
        TemplateRuntimeValue::HistogramBucket(value) => {
            reflection(&value.reflection(), verb, spec, 0)
        }
        TemplateRuntimeValue::HistogramIterator(value) => {
            reflection(&value.reflection(), verb, spec, 0)
        }
        TemplateRuntimeValue::HistogramSpan(value) => reflection(&span_view(value), verb, spec, 0),
        TemplateRuntimeValue::FloatSlice(values) => reflection(
            &super::super::TemplateHistogramView::slice_at(
                "[]float64",
                values.snapshot(),
                values.is_nil(),
                values.address(),
            ),
            verb,
            spec,
            0,
        ),
        TemplateRuntimeValue::HistogramSpans(values) => reflection(
            &super::super::TemplateHistogramView::slice_at(
                "[]histogram.Span",
                values.snapshot(),
                values.is_nil(),
                values.address(),
            ),
            verb,
            spec,
            0,
        ),
        TemplateRuntimeValue::Float(value) => {
            floating::render(*value, verb, spec).unwrap_or_else(invalid)
        }
        TemplateRuntimeValue::Complex(real, imaginary) => {
            if !matches!(
                verb,
                'v' | 'b' | 'e' | 'E' | 'f' | 'F' | 'g' | 'G' | 'x' | 'X'
            ) {
                return invalid();
            }
            let real = floating::render(*real, verb, spec).expect("complex float verb");
            let imaginary = floating::render(
                *imaginary,
                verb,
                Spec { ..spec }.with_flag(Spec::PLUS, true),
            )
            .expect("complex float verb");
            format!("({real}{imaginary}i)")
        }
        TemplateRuntimeValue::Json(value) => json(value, verb, spec, false),
        TemplateRuntimeValue::Time(value) => time(value, verb, spec),
        TemplateRuntimeValue::TimePointer(value) if verb == 'p' => {
            pointer(std::sync::Arc::as_ptr(value) as usize, spec)
        }
        TemplateRuntimeValue::TimePointer(value) => time(value, verb, spec),
        TemplateRuntimeValue::DurationPointer(value) if verb == 'p' => {
            pointer(std::sync::Arc::as_ptr(value) as usize, spec)
        }
        TemplateRuntimeValue::DurationPointer(value) => named_integer(
            **value,
            &super::super::template_time::duration::string(**value),
            verb,
            spec,
        ),
        TemplateRuntimeValue::Duration(value) => named_integer(
            *value,
            &super::super::template_time::duration::string(*value),
            verb,
            spec,
        ),
        TemplateRuntimeValue::Month(number) | TemplateRuntimeValue::Weekday(number) => {
            named_integer(i64::from(*number), &value.as_rendered_string(), verb, spec)
        }
        TemplateRuntimeValue::TimeLocation(value) if renders_as_text => {
            string(&value.name(), verb, spec)
        }
        TemplateRuntimeValue::TimeLocation(value) => reflection(&value.reflection(), verb, spec, 0),
        TemplateRuntimeValue::QueryError(_) => String::new(),
        TemplateRuntimeValue::NilError => {
            if verb == 'v' {
                spec.pad("<nil>")
            } else {
                format!("%!{verb}(<nil>)")
            }
        }
        TemplateRuntimeValue::HistogramError(value) if renders_as_text => {
            string(&value.message, verb, spec)
        }
        TemplateRuntimeValue::HistogramError(value) => {
            reflection(&error_view(value), verb, spec, 0)
        }
        TemplateRuntimeValue::SafeHtml(_)
        | TemplateRuntimeValue::QueryResult(_)
        | TemplateRuntimeValue::Sample(_)
        | TemplateRuntimeValue::ByteSlice(_)
        | TemplateRuntimeValue::Bytes(_)
        | TemplateRuntimeValue::ByteLabels(_)
        | TemplateRuntimeValue::Object(_)
        | TemplateRuntimeValue::Array(_) => {
            super::super::template_bytes_to_string(&super::raw::render(value, verb, spec, false))
        }
        TemplateRuntimeValue::Labels(values) => {
            let sharp = verb == 'v' && spec.sharp_v;
            let elements = values
                .iter()
                .map(|(key, value)| {
                    format!("{}:{}", string(key, verb, spec), string(value, verb, spec))
                })
                .collect::<Vec<_>>();
            if sharp {
                format!("map[string]string{{{}}}", elements.join(", "))
            } else {
                format!("map[{}]", elements.join(" "))
            }
        }
    }
}

fn time(value: &super::super::TemplateTime, verb: char, spec: Spec) -> String {
    if verb == 'v' && spec.sharp_v {
        return spec.pad(&truncate(&value.go_string(), spec.precision));
    }
    if matches!(verb, 'v' | 's' | 'q' | 'x' | 'X') {
        return string(&value.as_string(), verb, spec);
    }
    let (wall, ext, location) = value.reflection_fields();
    if verb == 'p' {
        return format!(
            "%!p(time.Time={{{wall} {ext} {}}})",
            location.map_or_else(|| "<nil>".into(), |pointer| pointer.to_string())
        );
    }
    let field = |bits: u64, signed: bool, name: &str| {
        integer(bits, signed && bits.cast_signed() < 0, signed, verb, spec).unwrap_or_else(|| {
            format!(
                "%!{verb}({name}={})",
                integer(bits, signed && bits.cast_signed() < 0, signed, 'v', spec)
                    .expect("v formats integers")
            )
        })
    };
    // Pointer values are process-local, as in Go. UTC uses its canonical nil
    // location; named locations use the native location identity.
    let pointer = location.map_or(0, |pointer| pointer as u64);
    let location = matches!(verb, 'v' | 'b' | 'd' | 'o' | 'x' | 'X')
        .then(|| integer(pointer, false, false, verb, spec))
        .flatten()
        .unwrap_or_else(|| {
            format!(
                "%!{verb}(*time.Location={})",
                if pointer == 0 {
                    "<nil>".into()
                } else {
                    format!("0x{pointer:x}")
                }
            )
        });
    format!(
        "{{{} {} {location}}}",
        field(wall, false, "uint64"),
        field(ext.cast_unsigned(), true, "int64")
    )
}

fn json(value: &Value, verb: char, spec: Spec, nested: bool) -> String {
    match value {
        Value::Null => {
            if nested && verb != 'v' {
                return spec.pad("<nil>");
            }
            if verb == 'v' {
                if nested && spec.sharp_v {
                    "interface {}(nil)".into()
                } else {
                    spec.pad("<nil>")
                }
            } else {
                format!("%!{verb}(<nil>)")
            }
        }
        Value::String(value) => string(value, verb, spec),
        Value::Number(value) => floating::render(value.as_f64().unwrap_or(0.0), verb, spec)
            .unwrap_or_else(|| {
                format!(
                    "%!{verb}(float64={})",
                    json(&Value::Number(value.clone()), 'v', spec, nested)
                )
            }),
        Value::Bool(value) => {
            if matches!(verb, 'v' | 't') {
                spec.pad(if *value { "true" } else { "false" })
            } else {
                format!(
                    "%!{verb}(bool={})",
                    json(&Value::Bool(*value), 'v', spec, nested)
                )
            }
        }
        Value::Array(values) => {
            let sharp = verb == 'v' && spec.sharp_v;
            let element_spec = spec;
            let elements = values
                .iter()
                .map(|value| json(value, verb, element_spec, true))
                .collect::<Vec<_>>();
            if sharp {
                format!("[]interface {{}}{{{}}}", elements.join(", "))
            } else {
                format!("[{}]", elements.join(" "))
            }
        }
        Value::Object(values) => {
            let sharp = verb == 'v' && spec.sharp_v;
            let element_spec = spec;
            let mut keys: Vec<_> = values.keys().collect();
            keys.sort_unstable();
            let elements = keys
                .iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        string(key, verb, element_spec),
                        json(&values[*key], verb, element_spec, true)
                    )
                })
                .collect::<Vec<_>>();
            if sharp {
                format!("map[string]interface {{}}{{{}}}", elements.join(", "))
            } else {
                format!("map[{}]", elements.join(" "))
            }
        }
    }
}

fn truncate(value: &str, precision: Option<usize>) -> String {
    precision.map_or_else(
        || value.into(),
        |precision| value.chars().take(precision).collect(),
    )
}

fn string(value: &str, verb: char, spec: Spec) -> String {
    let rendered = match verb {
        's' | 'v' if !(verb == 'v' && spec.sharp_v) => truncate(value, spec.precision),
        'q' | 'v' => {
            let value = truncate(value, spec.precision);
            if spec.flags[Spec::SHARP] && verb == 'q' && can_backquote(&value) {
                format!("`{value}`")
            } else {
                quote(&value, '"', spec.flags[Spec::PLUS] && verb == 'q')
            }
        }
        'x' | 'X' => {
            let bytes = &value.as_bytes()[..spec.precision.unwrap_or(value.len()).min(value.len())];
            let prefix = if verb == 'x' { "0x" } else { "0X" };
            let mut output = String::new();
            for (i, byte) in bytes.iter().enumerate() {
                if i > 0 && spec.flags[Spec::SPACE] {
                    output.push(' ');
                }
                if spec.flags[Spec::SHARP] && (i == 0 || spec.flags[Spec::SPACE]) {
                    output.push_str(prefix);
                }
                if verb == 'x' {
                    write!(output, "{byte:02x}").expect("String writes succeed");
                } else {
                    write!(output, "{byte:02X}").expect("String writes succeed");
                }
            }
            output
        }
        _ => return format!("%!{verb}(string={})", string(value, 'v', spec)),
    };
    spec.pad(&rendered)
}

fn integer(bits: u64, negative: bool, signed: bool, verb: char, mut spec: Spec) -> Option<String> {
    if matches!(verb, 'c' | 'q') {
        let ch = u32::try_from(bits)
            .ok()
            .and_then(char::from_u32)
            .unwrap_or(char::REPLACEMENT_CHARACTER);
        return Some(spec.pad(&if verb == 'c' {
            ch.to_string()
        } else {
            quote(&ch.to_string(), '\'', spec.flags[Spec::PLUS])
        }));
    }
    if verb == 'U' {
        spec.flags[Spec::ZERO] = false;
        let digits = spec.precision.unwrap_or(4).max(4);
        let mut output = format!("U+{bits:0digits$X}");
        if spec.flags[Spec::SHARP]
            && let Some(ch) = u32::try_from(bits)
                .ok()
                .and_then(char::from_u32)
                .filter(|ch| printable(*ch))
        {
            write!(output, " '{ch}'").expect("String writes succeed");
        }
        return Some(spec.pad(&output));
    }
    if !matches!(verb, 'v' | 'b' | 'd' | 'o' | 'O' | 'x' | 'X') {
        return None;
    }
    let sharp_v = verb == 'v' && spec.sharp_v;
    if verb == 'v' && sharp_v && !signed {
        spec.flags[Spec::SHARP] = true;
    }
    let magnitude = if negative { bits.wrapping_neg() } else { bits };
    if spec.precision == Some(0) && magnitude == 0 {
        spec.flags[Spec::ZERO] = false;
        return Some(spec.pad(""));
    }
    let base = match verb {
        'b' => 2,
        'o' | 'O' => 8,
        'x' | 'X' => 16,
        'v' if sharp_v && !signed => 16,
        _ => 10,
    };
    let mut digits = match base {
        2 => format!("{magnitude:b}"),
        8 => format!("{magnitude:o}"),
        16 if verb == 'X' => format!("{magnitude:X}"),
        16 => format!("{magnitude:x}"),
        _ => magnitude.to_string(),
    };
    let sign = if negative {
        "-"
    } else if spec.flags[Spec::PLUS] {
        "+"
    } else if spec.flags[Spec::SPACE] {
        " "
    } else {
        ""
    };
    let precision = spec.precision.unwrap_or_else(|| {
        if spec.flags[Spec::ZERO] && !spec.flags[Spec::MINUS] {
            spec.width.unwrap_or(0).saturating_sub(sign.len())
        } else {
            0
        }
    });
    if precision > digits.len() {
        digits = format!("{}{digits}", "0".repeat(precision - digits.len()));
    }
    let prefix = if spec.flags[Spec::SHARP] {
        match base {
            2 => "0b",
            8 if !digits.starts_with('0') => "0",
            16 if verb == 'X' => "0X",
            16 => "0x",
            _ => "",
        }
    } else {
        ""
    };
    let octal = if verb == 'O' { "0o" } else { "" };
    spec.flags[Spec::ZERO] = false;
    Some(spec.pad(&format!("{sign}{octal}{prefix}{digits}")))
}

fn can_backquote(value: &str) -> bool {
    // strconv.CanBackquote permits every valid multibyte rune except BOM,
    // even non-printable Unicode characters; ASCII controls are restricted.
    value.chars().all(|ch| {
        ch != '\u{feff}'
            && (!ch.is_ascii() || ((ch >= ' ' || ch == '\t') && ch != '`' && ch != '\u{7f}'))
    })
}

fn quote(value: &str, delimiter: char, ascii: bool) -> String {
    let mut output = String::new();
    output.push(delimiter);
    for ch in value.chars() {
        match ch {
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{7}' => output.push_str("\\a"),
            '\u{8}' => output.push_str("\\b"),
            '\u{b}' => output.push_str("\\v"),
            '\u{c}' => output.push_str("\\f"),
            ch if ch == delimiter => {
                output.push('\\');
                output.push(ch);
            }
            ch if (!ascii || ch.is_ascii()) && printable(ch) => output.push(ch),
            ch if ch < ' ' || ch == '\u{7f}' => {
                write!(output, "\\x{:02x}", u32::from(ch)).expect("String writes succeed");
            }
            ch if u32::from(ch) < 0x1_0000 => {
                write!(output, "\\u{:04x}", u32::from(ch)).expect("String writes succeed");
            }
            ch => {
                write!(output, "\\U{:08x}", u32::from(ch)).expect("String writes succeed");
            }
        }
    }
    output.push(delimiter);
    output
}

fn named_integer(value: i64, text: &str, verb: char, spec: Spec) -> String {
    if matches!(verb, 'v' | 's' | 'q' | 'x' | 'X') && !(verb == 'v' && spec.sharp_v) {
        return string(text, verb, spec);
    }
    integer(value.cast_unsigned(), value < 0, true, verb, spec)
        .unwrap_or_else(|| format!("%!{verb}(time.Duration={value})"))
}

fn span_view(value: &super::super::TemplateHistogramSpan) -> super::super::TemplateHistogramView {
    use super::super::{TemplateHistogramView as R, TemplateRuntimeValue as V};
    R::structure(
        "histogram.Span",
        vec![
            ("Offset", R::scalar(V::Integer32(value.offset))),
            ("Length", R::scalar(V::Unsigned32(value.length))),
        ],
    )
}
fn error_view(
    value: &std::sync::Arc<super::super::TemplateHistogramError>,
) -> super::super::TemplateHistogramView {
    use super::super::{TemplateHistogramView as R, TemplateRuntimeValue as V};
    let fields = if value.kind == "*errors.errorString" {
        vec![("s", R::scalar(V::String(value.message.clone())))]
    } else if value.kind == "histogram.Error" {
        vec![(
            "error",
            value
                .unwrap
                .as_ref()
                .map_or_else(|| R::scalar(V::NilError), error_view),
        )]
    } else {
        vec![
            ("msg", R::scalar(V::String(value.message.clone()))),
            (
                "err",
                value
                    .unwrap
                    .as_ref()
                    .map_or_else(|| R::scalar(V::NilError), error_view),
            ),
        ]
    };
    let object = R::structure(value.kind.strip_prefix('*').unwrap_or(value.kind), fields);
    if let Some(name) = value.kind.strip_prefix('*') {
        R::Pointer {
            name,
            address: std::sync::Arc::as_ptr(value) as usize,
            value: Box::new(object),
        }
    } else {
        object
    }
}
// fmt/print.go::printValue reflects private nested fields without invoking
// Stringer methods. Pointer addresses retain local object identity, as in Go.
fn reflection(
    view: &super::super::TemplateHistogramView,
    verb: char,
    spec: Spec,
    depth: usize,
) -> String {
    use super::super::{TemplateHistogramView as R, TemplateRuntimeValue as V};
    let sharp = verb == 'v' && spec.sharp_v;
    match view {
        R::Scalar(V::HistogramSpan(value)) => reflection(&span_view(value), verb, spec, depth),
        R::Scalar(value) => render(value, verb, spec),
        R::NamedScalar { name, value } => {
            let output = render(value, verb, spec);
            output.replace(
                &format!("%!{verb}({}=", type_name(value)),
                &format!("%!{verb}({name}="),
            )
        }
        R::Struct { name, fields } => {
            if verb == 'p' {
                return format!(
                    "%!p({name}={})",
                    reflection(view, 'v', Spec::default(), depth)
                );
            }
            let parts = fields
                .iter()
                .map(|(field, value)| {
                    let value = reflection(value, verb, spec, depth + 1);
                    if sharp || (verb == 'v' && spec.plus_v) {
                        format!("{field}:{value}")
                    } else {
                        value
                    }
                })
                .collect::<Vec<_>>();
            if sharp {
                format!("{name}{{{}}}", parts.join(", "))
            } else {
                format!("{{{}}}", parts.join(" "))
            }
        }
        R::Slice {
            name,
            values,
            is_nil,
            address,
        } => {
            if sharp && *is_nil {
                return format!("{name}(nil)");
            }
            if verb == 'p' {
                return pointer(if *is_nil { 0 } else { *address }, spec);
            }
            let values = values
                .iter()
                .map(|value| reflection(value, verb, spec, depth + 1))
                .collect::<Vec<_>>();
            if sharp {
                format!("{name}{{{}}}", values.join(", "))
            } else {
                format!("[{}]", values.join(" "))
            }
        }
        R::Pointer {
            name,
            address,
            value,
        } => {
            if verb == 'p' {
                return pointer(*address, spec);
            }
            if *address == 0 {
                if sharp {
                    return format!("(*{name})(nil)");
                }
                if verb == 'v' {
                    return spec.pad("<nil>");
                }
                if matches!(verb, 'b' | 'd' | 'o' | 'O' | 'x' | 'X') {
                    return integer(0, false, false, verb, spec).expect("pointer numeric verb");
                }
                return format!("%!{verb}(*{name}=<nil>)");
            }
            if depth == 0 {
                return format!("&{}", reflection(value, verb, spec, depth + 1));
            }
            if sharp {
                return format!("(*{name})(0x{address:x})");
            }
            if verb == 'v' {
                return pointer(*address, spec);
            }
            if matches!(verb, 'b' | 'd' | 'o' | 'O' | 'x' | 'X')
                && let Some(formatted) = integer(*address as u64, false, false, verb, spec)
            {
                return formatted;
            }
            format!(
                "%!{verb}(*{name}=&{})",
                reflection(value, 'v', Spec::default(), depth + 1)
            )
        }
    }
}

fn pointer(address: usize, spec: Spec) -> String {
    // Go fmt/print.go::fmtPointer -> fmt0x64: # suppresses the prefix;
    // sign, width, zero padding and precision retain integer formatting.
    integer(
        address as u64,
        false,
        false,
        'x',
        Spec {
            sharp_v: false,
            ..spec
        }
        .with_flag(Spec::SHARP, !spec.flags[Spec::SHARP]),
    )
    .expect("pointer hexadecimal verb")
}

#[cfg(test)]
mod pointer_tests {
    use super::*;
    #[test]
    fn pointer_flags_match_go_fixed_address_goldens() {
        // Go1.26.5 fmt.Sprintf against unsafe.Pointer(uintptr(0x1234));
        // formatting never dereferences this independently chosen address.
        for (spec, expected) in [
            (Spec::default(), "0x1234"),
            (
                Spec { ..Spec::default() }.with_flag(Spec::SHARP, true),
                "1234",
            ),
            (
                Spec { ..Spec::default() }.with_flag(Spec::PLUS, true),
                "+0x1234",
            ),
            (
                Spec { ..Spec::default() }.with_flag(Spec::SPACE, true),
                " 0x1234",
            ),
            (
                Spec {
                    width: Some(20),
                    ..Spec::default()
                }
                .with_flag(Spec::ZERO, true),
                "0x00000000000000001234",
            ),
            (
                Spec {
                    width: Some(20),
                    ..Spec::default()
                }
                .with_flag(Spec::SHARP, true)
                .with_flag(Spec::ZERO, true),
                "00000000000000001234",
            ),
            (
                Spec {
                    precision: Some(20),
                    ..Spec::default()
                },
                "0x00000000000000001234",
            ),
            (
                Spec {
                    width: Some(20),
                    precision: Some(6),
                    ..Spec::default()
                }
                .with_flag(Spec::ZERO, true),
                "            0x001234",
            ),
            (
                Spec {
                    width: Some(12),
                    ..Spec::default()
                }
                .with_flag(Spec::MINUS, true),
                "0x1234      ",
            ),
        ] {
            assert2::assert!(pointer(0x1234, spec) == expected);
        }
    }
}
