use base64::{Engine as _, prelude::BASE64_STANDARD};

use super::TemplateRuntimeValue;
mod byte_regex;
mod go_unicode_case;

/// Compares UTF-8 strings using Go's Unicode 15 simple case folding.
#[must_use]
pub fn go_string_equal_fold(left: &str, right: &str) -> bool {
    if left.is_ascii() && right.is_ascii() {
        return left.eq_ignore_ascii_case(right);
    }
    left.chars()
        .map(fold_go_rune)
        .eq(right.chars().map(fold_go_rune))
}

fn fold_go_rune(ch: char) -> char {
    match ch {
        // Go's locale-independent folding excludes the two Turkish I mappings.
        'İ' | 'ı' => ch,
        _ => go_unicode_case::convert(go_unicode_case::convert(ch, 0), 1),
    }
}

pub(crate) fn evaluate_template_byte_function(
    name: &str,
    args: &[TemplateRuntimeValue],
) -> Option<TemplateRuntimeValue> {
    let bytes = |index: usize| args[index].string_bytes().unwrap_or_default();
    let integer = |index: usize| args[index].signed_integer().unwrap_or_default();
    let boolean = |value| TemplateRuntimeValue::Json(serde_json::Value::Bool(value));
    let result = match name {
        "regexReplaceAll" | "regexReplaceAllLiteral" | "count" => {
            let result = byte_regex::regex_apply(
                name,
                bytes(0),
                bytes(1),
                if name == "count" { b"" } else { bytes(2) },
            )?;
            if name == "count" {
                return Some(TemplateRuntimeValue::Integer(
                    i64::try_from(result.matches).expect("regex matches fit int64"),
                ));
            }
            result.value
        }
        "reReplaceAll" => byte_regex::regex_apply(name, bytes(0), bytes(2), bytes(1))?.value,
        "urlquery" => query_escape(&super::format_template_print_bytes(args, false)),
        "html" => {
            let mut result = super::format_template_print_bytes(args, false);
            for replacement in [
                HtmlReplacement {
                    source: b"&",
                    replacement: b"&amp;",
                },
                HtmlReplacement {
                    source: b"<",
                    replacement: b"&lt;",
                },
                HtmlReplacement {
                    source: b">",
                    replacement: b"&gt;",
                },
                HtmlReplacement {
                    source: b"\"",
                    replacement: b"&#34;",
                },
                HtmlReplacement {
                    source: b"'",
                    replacement: b"&#39;",
                },
            ] {
                result = replace_bytes(&result, replacement.source, replacement.replacement);
            }
            result
        }
        "b64enc" => BASE64_STANDARD.encode(bytes(0)).into_bytes(),
        "b64dec" => decode_base64(bytes(0)),
        "contains" => return Some(boolean(find_bytes(bytes(1), bytes(0)).is_some())),
        "hasPrefix" => return Some(boolean(bytes(1).starts_with(bytes(0)))),
        "hasSuffix" => return Some(boolean(bytes(1).ends_with(bytes(0)))),
        "replace" => replace_bytes(bytes(2), bytes(0), bytes(1)),
        "trimPrefix" => bytes(1).strip_prefix(bytes(0)).unwrap_or(bytes(1)).to_vec(),
        "trimSuffix" => bytes(1).strip_suffix(bytes(0)).unwrap_or(bytes(1)).to_vec(),
        "trim" => trim_runes(bytes(0), char::is_whitespace),
        "trimAll" => {
            let set = runes(bytes(0))
                .into_iter()
                .map(|rune| rune.character)
                .collect::<Vec<_>>();
            trim_runes(bytes(1), |ch| set.contains(&ch))
        }
        "lower" | "upper" | "title" => {
            let mut previous = ' ';
            let mut result = String::new();
            for rune in runes(bytes(0)) {
                let ch = rune.character;
                let mapped = match name {
                    "lower" => go_unicode_case::convert(ch, 1),
                    "upper" => go_unicode_case::convert(ch, 0),
                    _ if is_separator(previous) => go_unicode_case::convert(ch, 2),
                    _ => ch,
                };
                previous = ch;
                result.push(mapped);
            }
            result.into_bytes()
        }
        "trunc" => {
            let value = bytes(1);
            let count = integer(0);
            if count >= 0 {
                value[..usize::try_from(count)
                    .unwrap_or(usize::MAX)
                    .min(value.len())]
                    .to_vec()
            } else {
                let keep = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
                value[value.len().saturating_sub(keep)..].to_vec()
            }
        }
        "substr" => {
            let value = bytes(2);
            let start = integer(0);
            let end = integer(1);
            let begin = usize::try_from(start).unwrap_or(0);
            let end = usize::try_from(end)
                .ok()
                .filter(|end| *end <= value.len())
                .unwrap_or(value.len());
            value.get(begin..end).unwrap_or_default().to_vec()
        }
        "repeat" => bytes(1).repeat(usize::try_from(integer(0)).unwrap_or_default()),
        "indent" | "nindent" => {
            let padding = vec![b' '; usize::try_from(integer(0)).unwrap_or_default()];
            let mut result = Vec::new();
            if name == "nindent" {
                result.push(b'\n');
            }
            result.extend(&padding);
            for (line_index, part) in bytes(1).split(|byte| *byte == b'\n').enumerate() {
                if line_index > 0 {
                    result.push(b'\n');
                    result.extend(&padding);
                }
                result.extend(part);
            }
            result
        }
        "alignLeft" | "alignRight" => {
            let value = bytes(1);
            let width = integer(0);
            let chars = runes(value);
            let Ok(width) = usize::try_from(width) else {
                return Some(TemplateRuntimeValue::Bytes(value.to_vec()));
            };
            if width >= chars.len() {
                let padding = vec![b' '; width - chars.len()];
                if name == "alignLeft" {
                    [value, padding.as_slice()].concat()
                } else {
                    [padding.as_slice(), value].concat()
                }
            } else {
                let selected = if name == "alignLeft" {
                    &chars[..width]
                } else {
                    &chars[chars.len() - width..]
                };
                selected
                    .iter()
                    .map(|rune| rune.character)
                    .collect::<String>()
                    .into_bytes()
            }
        }
        "urlencode" => query_escape(bytes(0)),
        "urldecode" => {
            let value = bytes(0);
            let mut result = Vec::new();
            let mut pos = 0;
            while pos < value.len() {
                match value[pos] {
                    b'+' => result.push(b' '),
                    b'%' if pos + 2 < value.len() => {
                        let pair = std::str::from_utf8(&value[pos + 1..pos + 3]).ok()?;
                        result.push(u8::from_str_radix(pair, 16).ok()?);
                        pos += 2;
                    }
                    byte => result.push(byte),
                }
                pos += 1;
            }
            result
        }
        _ => return None,
    };
    Some(TemplateRuntimeValue::Bytes(result))
}

fn find_bytes(value: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        Some(0)
    } else {
        value
            .windows(needle.len())
            .position(|window| window == needle)
    }
}

fn replace_bytes(value: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    if needle.is_empty() {
        result.extend(replacement);
        for rune in runes(value) {
            result.extend(&value[rune.start..rune.start + rune.length]);
            result.extend(replacement);
        }
        return result;
    }
    let mut remaining = value;
    while let Some(index) = find_bytes(remaining, needle) {
        result.extend(&remaining[..index]);
        result.extend(replacement);
        remaining = &remaining[index + needle.len()..];
    }
    result.extend(remaining);
    result
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Rune {
    pub character: char,
    pub start: usize,
    pub length: usize,
}

struct DecodedRune {
    character: char,
    length: usize,
}

struct BytePosition {
    index: usize,
    byte: u8,
}

struct HtmlReplacement {
    source: &'static [u8],
    replacement: &'static [u8],
}

pub(super) fn runes(value: &[u8]) -> Vec<Rune> {
    let mut chars = Vec::new();
    let mut pos = 0;
    while pos < value.len() {
        let length = match value[pos] {
            0xc2..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf4 => 4,
            _ => 1,
        };
        let valid = value
            .get(pos..pos + length)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|text| text.chars().next());
        let decoded = valid.map_or(
            DecodedRune {
                character: '\u{fffd}',
                length: 1,
            },
            |character| DecodedRune { character, length },
        );
        chars.push(Rune {
            character: decoded.character,
            start: pos,
            length: decoded.length,
        });
        pos += decoded.length;
    }
    chars
}

fn trim_runes(value: &[u8], predicate: impl Fn(char) -> bool) -> Vec<u8> {
    let chars = runes(value);
    let Some(first) = chars.iter().position(|rune| !predicate(rune.character)) else {
        return Vec::new();
    };
    let last = chars
        .iter()
        .rposition(|rune| !predicate(rune.character))
        .expect("nonempty trimmed value");
    value[chars[first].start..chars[last].start + chars[last].length].to_vec()
}

fn is_separator(ch: char) -> bool {
    if ch.is_ascii() {
        !(ch.is_ascii_alphanumeric() || ch == '_')
    } else {
        ch.is_whitespace()
    }
}

fn query_escape(value: &[u8]) -> Vec<u8> {
    const HEX: &[u8] = b"0123456789ABCDEF";
    let mut result = Vec::new();
    for &byte in value {
        if byte.is_ascii_alphanumeric() || b"-_.~".contains(&byte) {
            result.push(byte);
        } else if byte == b' ' {
            result.push(b'+');
        } else {
            result.extend([
                b'%',
                HEX[usize::from(byte >> 4)],
                HEX[usize::from(byte & 15)],
            ]);
        }
    }
    result
}

fn decode_base64(value: &[u8]) -> Vec<u8> {
    let mut positions = Vec::new();
    for (index, byte) in value.iter().copied().enumerate() {
        if byte != b'\r' && byte != b'\n' {
            positions.push(BytePosition { index, byte });
        }
    }
    let input = positions
        .iter()
        .map(|position| position.byte)
        .collect::<Vec<_>>();
    BASE64_STANDARD.decode(&input).unwrap_or_else(|error| {
        let index = match error {
            base64::DecodeError::InvalidByte(index, _)
            | base64::DecodeError::InvalidLastSymbol { offset: index, .. } => index,
            base64::DecodeError::InvalidLength(length) => length.saturating_sub(length % 4),
            base64::DecodeError::InvalidPadding => input
                .iter()
                .position(|byte| *byte == b'=')
                .unwrap_or(input.len()),
        };
        let index = positions
            .get(index)
            .map_or(value.len(), |position| position.index);
        format!("illegal base64 data at input byte {index}").into_bytes()
    })
}

// Go's UTF-8 decoder replaces every invalid byte, including each byte of an
// incomplete sequence; Rust's lossy decoder coalesces some sequences.
pub(crate) fn template_bytes_to_string(value: &[u8]) -> String {
    runes(value)
        .into_iter()
        .map(|rune| rune.character)
        .collect()
}

#[cfg(test)]
mod fold_tests {
    use super::go_string_equal_fold;

    #[test]
    fn canonical_fold_matches_the_independent_go_unicode_15_ledger() {
        // FNV-1a over little-endian Go 1.26.5 ToLower(ToUpper(r)) values,
        // excluding Turkish I mappings and invalid Unicode scalar values.
        let mut digest = 14_695_981_039_346_656_037_u64;
        for rune in 0..=0x10_ffff {
            if let Some(ch) = char::from_u32(rune) {
                for byte in u32::from(super::fold_go_rune(ch)).to_le_bytes() {
                    digest = (digest ^ u64::from(byte)).wrapping_mul(1_099_511_628_211);
                }
            }
        }
        assert2::assert!(digest == 12_005_982_000_727_758_298);
    }

    // Independent Go 1.26.5 strings.EqualFold results; the pinned cases and
    // canonicalization were checked over every Unicode 15 scalar.
    #[yare::parameterized(
        long_s = { "ſeverity", "Severity", true },
        kelvin = { "\u{212a}ey", "key", true },
        sigma = { "Σ", "ς", true },
        sharp_s = { "ẞ", "ß", true },
        no_expansion = { "ß", "SS", false },
        dotted_i = { "İ", "i", false },
        dotless_i = { "ı", "I", false },
        length_mismatch = { "key", "keys", false },
    )]
    fn simple_folding_keeps_unicode_orbits_and_rejects_expansions(
        left: &str,
        right: &str,
        expected: bool,
    ) {
        assert2::assert!(go_string_equal_fold(left, right) == expected);
    }
}
