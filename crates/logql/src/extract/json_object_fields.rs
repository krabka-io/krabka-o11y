use std::fmt;

use serde::de::{MapAccess, Visitor};
use serde_json::value::RawValue;

/// Reads object entries in their physical input order, retaining duplicate keys
/// and borrowing the original JSON value text. Text following the first complete
/// object is ignored, matching Loki's `ObjectEach` parser.
///
/// # Errors
/// Returns the JSON decoding error for malformed input or a non-object root.
pub fn parse_json_object_entries(
    line: &str,
) -> Result<Vec<(String, &RawValue)>, serde_json::Error> {
    let mut parser = serde_json::Deserializer::from_str(line);
    serde::Deserializer::deserialize_map(&mut parser, ObjectEntries)
}

struct ObjectEntries;

impl<'de> Visitor<'de> for ObjectEntries {
    type Value = Vec<(String, &'de RawValue)>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut entries = Vec::new();
        while let Some(name) = map.next_key::<String>()? {
            entries.push((name, map.next_value::<&RawValue>()?));
        }
        Ok(entries)
    }
}
