use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
};

/// A Go string: arbitrary bytes whose identity is independent of JSON rendering.
#[derive(Clone)]
pub struct MetricString {
    // Valid UTF-8 uses the JSON buffer for both views.
    bytes: Option<Vec<u8>>,
    json: String,
}

impl MetricString {
    /// Returns the original bytes, including invalid UTF-8 sequences.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes.as_deref().unwrap_or(self.json.as_bytes())
    }

    /// Returns the string representation used by Go's JSON encoder.
    ///
    /// Every invalid byte is replaced by U+FFFD. This view must not be used
    /// for label identity, grouping, matching, or capture-group replacement.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.json
    }

    /// Returns the original UTF-8 string, when the bytes are valid UTF-8.
    #[must_use]
    pub fn utf8(&self) -> Option<&str> {
        self.bytes.is_none().then_some(self.json.as_str())
    }

    #[must_use]
    pub fn bytes_for_json_range(&self, start: usize, end: usize) -> Option<&[u8]> {
        let bytes = self.as_bytes();
        let offset = |target: usize| {
            let mut raw = 0;
            let mut json = 0;
            while raw < bytes.len() {
                let valid = match std::str::from_utf8(&bytes[raw..]) {
                    Ok(_) => bytes.len() - raw,
                    Err(error) => error.valid_up_to(),
                };
                if target <= json + valid {
                    return Some(raw + target - json);
                }
                raw += valid;
                json += valid;
                if raw == bytes.len() {
                    break;
                }
                if target < json + 3 {
                    return None;
                }
                raw += 1;
                json += 3;
            }
            (target == json).then_some(raw)
        };
        bytes.get(offset(start)?..offset(end)?)
    }

    /// Quotes the original bytes as a reparsable `PromQL` string literal.
    #[must_use]
    pub fn quoted(&self) -> String {
        let mut result = String::from("\"");
        for &byte in self.as_bytes() {
            match byte {
                b'"' => result.push_str("\\\""),
                b'\\' => result.push_str("\\\\"),
                b'\n' => result.push_str("\\n"),
                b'\r' => result.push_str("\\r"),
                b'\t' => result.push_str("\\t"),
                0x20..=0x7e => result.push(char::from(byte)),
                _ => {
                    use std::fmt::Write;
                    write!(result, "\\x{byte:02x}").expect("writing a string cannot fail");
                }
            }
        }
        result.push('"');
        result
    }
}

impl AsRef<[u8]> for MetricString {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<str> for MetricString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<Vec<u8>> for MetricString {
    fn from(bytes: Vec<u8>) -> Self {
        let bytes = match String::from_utf8(bytes) {
            Ok(json) => return Self { bytes: None, json },
            Err(error) => error.into_bytes(),
        };
        let mut json = String::new();
        let mut remaining = bytes.as_slice();
        while !remaining.is_empty() {
            match std::str::from_utf8(remaining) {
                Ok(valid) => {
                    json.push_str(valid);
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    json.push_str(
                        std::str::from_utf8(&remaining[..valid]).expect("validated prefix"),
                    );
                    json.push('\u{fffd}');
                    remaining = &remaining[valid + 1..];
                }
            }
        }
        Self {
            bytes: Some(bytes),
            json,
        }
    }
}

impl From<String> for MetricString {
    fn from(json: String) -> Self {
        Self { bytes: None, json }
    }
}
impl From<&str> for MetricString {
    fn from(value: &str) -> Self {
        value.to_owned().into()
    }
}
impl From<&String> for MetricString {
    fn from(value: &String) -> Self {
        value.as_str().into()
    }
}
impl From<&Self> for MetricString {
    fn from(value: &Self) -> Self {
        value.clone()
    }
}
impl Default for MetricString {
    fn default() -> Self {
        String::new().into()
    }
}
impl Deref for MetricString {
    type Target = str;
    fn deref(&self) -> &str {
        self.as_str()
    }
}
impl fmt::Display for MetricString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}
impl fmt::Debug for MetricString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricString")
            .field("bytes", &self.as_bytes())
            .field("json", &self.json)
            .finish()
    }
}
impl PartialEq for MetricString {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl Eq for MetricString {}
impl PartialEq<str> for MetricString {
    fn eq(&self, other: &str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl PartialEq<&str> for MetricString {
    fn eq(&self, other: &&str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl PartialEq<String> for MetricString {
    fn eq(&self, other: &String) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}
impl PartialOrd for MetricString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MetricString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}
impl Hash for MetricString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

// Internal codecs preserve the exact byte sequence. HTTP text conversion is explicit.
impl serde::Serialize for MetricString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable()
            && let Some(value) = self.utf8()
        {
            serializer.serialize_str(value)
        } else {
            serde::Serialize::serialize(self.as_bytes(), serializer)
        }
    }
}
impl<'de> serde::Deserialize<'de> for MetricString {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            #[derive(serde::Deserialize)]
            #[serde(untagged)]
            enum Value {
                Utf8(String),
                Bytes(Vec<u8>),
            }
            Ok(match Value::deserialize(deserializer)? {
                Value::Utf8(value) => value.into(),
                Value::Bytes(value) => value.into(),
            })
        } else {
            Ok(<Vec<u8> as serde::Deserialize>::deserialize(deserializer)?.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_identity_survives_json_replacement_and_internal_cache_round_trips() {
        let a = MetricString::from(vec![0xff]);
        let b = MetricString::from(vec![0xfe]);
        let unicode = MetricString::from("\u{fffd}");
        assert2::assert!(a != b && a != unicode && a.as_str() == b.as_str());
        assert2::assert!(MetricString::from(vec![0xe2, 0x82]).as_str() == "\u{fffd}\u{fffd}");
        let cached = serde_json::to_value(&a).unwrap();
        assert2::assert!(serde_json::from_value::<MetricString>(cached).unwrap() == a);
        assert2::assert!(a.quoted() == "\"\\xff\"");
    }
}
