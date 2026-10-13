use super::{MetricLabel, TraceMetricExemplar};

/// One timestamped sample in a metrics series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricPoint {
    pub timestamp: i64,
    pub value: f64,
}

impl serde::Serialize for MetricPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple as _;

        let mut point = serializer.serialize_tuple(2)?;
        point.serialize_element(&self.timestamp)?;
        point.serialize_element(&self.value)?;
        point.end()
    }
}

impl<'de> serde::Deserialize<'de> for MetricPoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MetricPointVisitor;

        impl<'de> serde::de::Visitor<'de> for MetricPointVisitor {
            type Value = MetricPoint;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a timestamp and metric value")
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let timestamp = sequence
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(0, &self))?;
                let value = sequence
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(1, &self))?;
                Ok(MetricPoint { timestamp, value })
            }
        }

        deserializer.deserialize_tuple(2, MetricPointVisitor)
    }
}

impl MetricPoint {
    #[must_use]
    pub const fn new(timestamp: i64, value: f64) -> Self {
        Self { timestamp, value }
    }
}

/// One `TraceQL` metrics series.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TraceMetricSeries {
    pub labels: Vec<MetricLabel>,
    /// Scalar types that cannot be recovered from a display label. Omitted
    /// entries are strings; e.g. integer 1 and string "1" remain distinct.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub label_types: std::collections::BTreeMap<String, super::TraceMetricLabelType>,
    pub points: Vec<MetricPoint>,
    pub exemplars: Vec<TraceMetricExemplar>,
}
