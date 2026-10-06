use std::ops::Range;

use super::{super::TemplateRuntimeValue, Spec, printable::printable, scalar};

/// Go utf8.DecodeRune consumes one byte for each malformed sequence.
pub(in crate::template) fn runes(bytes: &[u8]) -> Vec<(char, Range<usize>)> {
    let mut result = Vec::new();
    let mut position = 0;
    while position < bytes.len() {
        let (valid, invalid) = match std::str::from_utf8(&bytes[position..]) {
            Ok(value) => (value, false),
            Err(error) => (
                std::str::from_utf8(&bytes[position..position + error.valid_up_to()])
                    .expect("validated UTF-8 prefix"),
                true,
            ),
        };
        for ch in valid.chars() {
            let end = position + ch.len_utf8();
            result.push((ch, position..end));
            position = end;
        }
        if invalid {
            result.push((char::REPLACEMENT_CHARACTER, position..position + 1));
            position += 1;
        }
    }
    result
}

pub(super) fn render(
    value: &TemplateRuntimeValue,
    verb: char,
    spec: Spec,
    nested: bool,
) -> Vec<u8> {
    use TemplateRuntimeValue as V;
    if verb == 'T' {
        return scalar::render(value, verb, spec).into_bytes();
    }
    match value {
        V::Reference(value) => render(&value.resolve(), verb, spec, nested),
        V::ByteSlice(bytes) => {
            if matches!(verb, 's' | 'q' | 'x' | 'X') {
                return string(bytes, verb, spec);
            }
            if verb == 'v' && spec.sharp_v {
                return format!(
                    "[]byte{{{}}}",
                    bytes
                        .iter()
                        .map(|value| format!("0x{value:x}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
                .into_bytes();
            }
            render(
                &V::Array(bytes.iter().copied().map(V::Byte).collect()),
                verb,
                spec,
                nested,
            )
        }
        V::Bytes(value) | V::SafeHtml(value) => string(value, verb, spec),
        V::Sample(values) => {
            let mut output = b"&{".to_vec();
            if spec.flags[Spec::PLUS] && verb == 'v' {
                output.extend(b"Labels:");
            }
            output.extend(render(
                values
                    .get("Labels")
                    .unwrap_or(&V::Json(serde_json::Value::Null)),
                verb,
                spec,
                true,
            ));
            output.push(b' ');
            if spec.flags[Spec::PLUS] && verb == 'v' {
                output.extend(b"Value:");
            }
            output.extend(render(
                values
                    .get("Value")
                    .unwrap_or(&V::Json(serde_json::Value::Null)),
                verb,
                spec,
                true,
            ));
            output.push(b'}');
            output
        }
        V::String(value) | V::Json(serde_json::Value::String(value)) => {
            string(value.as_bytes(), verb, spec)
        }
        V::ByteLabels(values) => map(
            values
                .iter()
                .map(|(key, value)| (key.as_str(), V::Bytes(value.clone()))),
            "map[string]string",
            verb,
            spec,
        ),
        V::Object(values) => map(
            values
                .iter()
                .map(|(key, value)| (key.as_str(), value.clone())),
            "map[string]interface {}",
            verb,
            spec,
        ),
        V::QueryResult(values) => render(&V::Array(values.snapshot()), verb, spec, nested),
        V::Array(values) => {
            let sharp = verb == 'v' && spec.sharp_v;
            let mut output = if sharp {
                b"[]interface {}{".to_vec()
            } else {
                b"[".to_vec()
            };
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.extend_from_slice(if sharp { b", " } else { b" " });
                }
                output.extend_from_slice(&render(value, verb, spec, true));
            }
            output.push(if sharp { b'}' } else { b']' });
            output
        }
        V::Json(serde_json::Value::Null) if nested => {
            if verb == 'v' && spec.sharp_v {
                b"interface {}(nil)".to_vec()
            } else {
                spec.pad("<nil>").into_bytes()
            }
        }
        _ => scalar::render(value, verb, spec).into_bytes(),
    }
}

fn map<'a>(
    values: impl Iterator<Item = (&'a str, TemplateRuntimeValue)>,
    name: &str,
    verb: char,
    spec: Spec,
) -> Vec<u8> {
    let sharp = verb == 'v' && spec.sharp_v;
    let mut output = if sharp {
        format!("{name}{{").into_bytes()
    } else {
        b"map[".to_vec()
    };
    for (index, (key, value)) in values.enumerate() {
        if index > 0 {
            output.extend_from_slice(if sharp { b", " } else { b" " });
        }
        output.extend_from_slice(&string(key.as_bytes(), verb, spec));
        output.push(b':');
        output.extend_from_slice(&render(&value, verb, spec, true));
    }
    output.push(if sharp { b'}' } else { b']' });
    output
}

pub(super) fn string(value: &[u8], verb: char, spec: Spec) -> Vec<u8> {
    let units = runes(value);
    let length = spec
        .precision
        .and_then(|precision| units.get(precision).map(|unit| unit.1.start))
        .unwrap_or(value.len());
    let truncated = &value[..length];
    let output = match verb {
        's' | 'v' if !(verb == 'v' && spec.sharp_v) => truncated.to_vec(),
        'q' | 'v' => {
            if verb == 'q'
                && spec.flags[Spec::SHARP]
                && std::str::from_utf8(truncated).is_ok_and(|value| {
                    value.chars().all(|ch| {
                        ch != '\u{feff}'
                            && (!ch.is_ascii()
                                || ((ch >= ' ' || ch == '\t') && ch != '`' && ch != '\u{7f}'))
                    })
                })
            {
                [b"`".as_slice(), truncated, b"`"].concat()
            } else {
                let mut output = vec![b'"'];
                for (ch, range) in runes(truncated) {
                    let bytes = &truncated[range];
                    if ch == char::REPLACEMENT_CHARACTER && bytes.len() == 1 && bytes[0] >= 128 {
                        output.extend_from_slice(format!("\\x{:02x}", bytes[0]).as_bytes());
                        continue;
                    }
                    let escaped = match ch {
                        '\\' => "\\\\".into(),
                        '"' => "\\\"".into(),
                        '\n' => "\\n".into(),
                        '\r' => "\\r".into(),
                        '\t' => "\\t".into(),
                        '\u{7}' => "\\a".into(),
                        '\u{8}' => "\\b".into(),
                        '\u{b}' => "\\v".into(),
                        '\u{c}' => "\\f".into(),
                        ch if (!spec.flags[Spec::PLUS] || verb != 'q' || ch.is_ascii())
                            && printable(ch) =>
                        {
                            ch.to_string()
                        }
                        ch if ch < ' ' || ch == '\u{7f}' => format!("\\x{:02x}", ch as u32),
                        ch if (ch as u32) < 0x10000 => format!("\\u{:04x}", ch as u32),
                        ch => format!("\\U{:08x}", ch as u32),
                    };
                    output.extend_from_slice(escaped.as_bytes());
                }
                output.push(b'"');
                output
            }
        }
        'x' | 'X' => {
            let mut output = Vec::new();
            for (index, byte) in value[..spec.precision.unwrap_or(value.len()).min(value.len())]
                .iter()
                .enumerate()
            {
                if index > 0 && spec.flags[Spec::SPACE] {
                    output.push(b' ');
                }
                if spec.flags[Spec::SHARP] && (index == 0 || spec.flags[Spec::SPACE]) {
                    output.extend_from_slice(if verb == 'x' { b"0x" } else { b"0X" });
                }
                output.extend_from_slice(
                    if verb == 'x' {
                        format!("{byte:02x}")
                    } else {
                        format!("{byte:02X}")
                    }
                    .as_bytes(),
                );
            }
            output
        }
        _ => {
            return [
                format!("%!{verb}(string=").as_bytes(),
                &string(value, 'v', spec),
                b")",
            ]
            .concat();
        }
    };
    let padding = spec.width.unwrap_or(0).saturating_sub(runes(&output).len());
    if spec.flags[Spec::MINUS] {
        [output, vec![b' '; padding]].concat()
    } else {
        [
            vec![if spec.flags[Spec::ZERO] { b'0' } else { b' ' }; padding],
            output,
        ]
        .concat()
    }
}
