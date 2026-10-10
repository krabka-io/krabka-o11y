use std::fmt::{self, Display, Formatter};

use serde::{Serialize, Serializer};

/// A numeric timestamp until encoding, or an unchanged noncanonical string.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LokiTimestamp(TimestampValue);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum TimestampValue {
    Nanoseconds(i64),
    Text(String),
}

impl LokiTimestamp {
    /// Nanoseconds, or `None` if the retained text cannot be parsed.
    pub fn parsed(&self) -> Option<i64> {
        match &self.0 {
            TimestampValue::Nanoseconds(value) => Some(*value),
            TimestampValue::Text(value) => value.parse().ok(),
        }
    }
}

impl From<i64> for LokiTimestamp {
    fn from(value: i64) -> Self {
        Self(TimestampValue::Nanoseconds(value))
    }
}

impl From<String> for LokiTimestamp {
    fn from(value: String) -> Self {
        // Only canonical strings share the numeric identity. Preserve spelling
        // for malformed and noncanonical timestamps and their deduplication.
        if let Ok(timestamp) = value.parse::<i64>()
            && timestamp.to_string() == value
        {
            return Self::from(timestamp);
        }
        Self(TimestampValue::Text(value))
    }
}

impl From<&str> for LokiTimestamp {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

impl From<LokiTimestamp> for String {
    fn from(value: LokiTimestamp) -> Self {
        match value.0 {
            TimestampValue::Nanoseconds(value) => value.to_string(),
            TimestampValue::Text(value) => value,
        }
    }
}

impl Display for LokiTimestamp {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.0 {
            TimestampValue::Nanoseconds(value) => value.fmt(formatter),
            TimestampValue::Text(value) => value.fmt(formatter),
        }
    }
}

impl Serialize for LokiTimestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match &self.0 {
            TimestampValue::Nanoseconds(value) => serializer.collect_str(value),
            TimestampValue::Text(value) => serializer.serialize_str(value),
        }
    }
}

#[cfg(test)]
mod tests;
