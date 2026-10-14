use std::fmt;

use num_traits::ToPrimitive;
use serde::{Deserialize as _, Deserializer, de::Visitor};

pub(super) use crate::float_json::serialize_attr as serialize;

pub(super) fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
    if !deserializer.is_human_readable() {
        return f64::deserialize(deserializer);
    }
    deserializer.deserialize_any(FloatVisitor)
}

struct FloatVisitor;

impl Visitor<'_> for FloatVisitor {
    type Value = f64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an OTLP double")
    }

    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<f64, E> {
        Ok(value)
    }

    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<f64, E> {
        value
            .to_f64()
            .ok_or_else(|| E::custom("integer cannot be represented as a double"))
    }

    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<f64, E> {
        value
            .to_f64()
            .ok_or_else(|| E::custom("integer cannot be represented as a double"))
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<f64, E> {
        value.parse().map_err(E::custom)
    }
}
