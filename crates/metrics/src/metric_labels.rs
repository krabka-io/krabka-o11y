use std::collections::BTreeMap;

use crate::MetricString;

/// Query labels with Go string byte values, converted from UTF-8 storage labels.
#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MetricLabels(BTreeMap<String, MetricString>);

/// An owned name and value used to build metric labels.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct MetricLabelPair<V> {
    pub name: String,
    pub value: V,
}

impl<V> MetricLabelPair<V> {
    #[must_use]
    pub fn new(name: impl Into<String>, value: V) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// A borrowed name and value from a metric label set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetricLabelRef<'a> {
    pub name: &'a String,
    pub value: &'a MetricString,
}

impl MetricLabels {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn from_pairs<I, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = MetricLabelPair<V>>,
        V: Into<MetricString>,
    {
        let mut labels = BTreeMap::new();
        for pair in pairs {
            labels.insert(pair.name, pair.value.into());
        }
        Self(labels)
    }
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<MetricString>) {
        self.0.insert(name.into(), value.into());
    }
    pub fn remove(&mut self, name: &str) -> Option<MetricString> {
        self.0.remove(name)
    }
    /// Returns a label's JSON view. Byte-sensitive operations use `get_value`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(MetricString::as_str)
    }
    #[must_use]
    pub fn get_value(&self, name: &str) -> Option<&MetricString> {
        self.0.get(name)
    }
    pub fn iter(&self) -> impl Iterator<Item = MetricLabelRef<'_>> {
        self.0.iter().map(|pair| MetricLabelRef {
            name: pair.0,
            value: pair.1,
        })
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    /// Converts labels for a storage writer that requires UTF-8 values.
    ///
    /// Returns an execution error rather than merging distinct invalid-byte values.
    ///
    /// # Errors
    /// Returns an error if a label value cannot be stored as UTF-8.
    pub fn to_storage_labels(&self) -> Result<krabka_blockstore::Labels, String> {
        self.iter()
            .map(|pair| {
                let name = pair.name;
                let value = pair.value;
                let value = value
                    .utf8()
                    .ok_or_else(|| "metric storage requires UTF-8 label values".to_owned())?;
                Ok(krabka_blockstore::LabelPair::new(
                    name.clone(),
                    value.to_owned(),
                ))
            })
            .collect()
    }
    /// Returns only UTF-8 label values for the generic posting index.
    /// The caller retains this metric label set as the authoritative identity.
    #[must_use]
    pub fn utf8_projection(&self) -> krabka_blockstore::Labels {
        self.iter()
            .filter_map(|pair| {
                let name = pair.name;
                let value = pair.value;
                value
                    .utf8()
                    .map(|value| krabka_blockstore::LabelPair::new(name.clone(), value.to_owned()))
            })
            .collect()
    }
    #[must_use]
    pub fn has_byte_values(&self) -> bool {
        self.iter().any(|pair| pair.value.utf8().is_none())
    }
    /// Canonical length-prefixed byte identity, shared by collision and matching keys.
    #[must_use]
    pub fn byte_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        for pair in self.iter() {
            let name = pair.name;
            let value = pair.value;
            key.extend_from_slice(&(name.len() as u64).to_le_bytes());
            key.extend_from_slice(name.as_bytes());
            key.extend_from_slice(&(value.as_bytes().len() as u64).to_le_bytes());
            key.extend_from_slice(value.as_bytes());
        }
        key
    }
    /// A lossless lexical key over label names and original value bytes.
    /// Hex digits preserve byte order; a separator smaller than every digit
    /// preserves the ordering of a shorter string before its extensions.
    #[must_use]
    pub fn order_key(&self) -> String {
        use std::fmt::Write;
        let mut key = String::new();
        for pair in self.iter() {
            let name = pair.name;
            let value = pair.value;
            for bytes in [name.as_bytes(), value.as_bytes()] {
                for byte in bytes {
                    write!(key, "{byte:02x}").expect("writing a string cannot fail");
                }
                key.push('/');
            }
        }
        key
    }
    /// Computes the storage-compatible FNV-1a fingerprint over original label bytes.
    #[must_use]
    pub fn fingerprint(&self) -> krabka_blockstore::SeriesFingerprint {
        let mut hash = 0xcbf2_9ce4_8422_2325;
        for pair in self.iter() {
            let name = pair.name;
            let value = pair.value;
            for bytes in [name.as_bytes(), value.as_bytes()] {
                for byte in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
                    hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
                }
            }
        }
        hash
    }
}
impl From<krabka_blockstore::Labels> for MetricLabels {
    fn from(labels: krabka_blockstore::Labels) -> Self {
        Self::from_pairs(
            labels
                .into_iter()
                .map(|pair| MetricLabelPair::new(pair.name, pair.value)),
        )
    }
}
impl From<&krabka_blockstore::Labels> for MetricLabels {
    fn from(labels: &krabka_blockstore::Labels) -> Self {
        Self::from_pairs(
            labels
                .iter()
                .map(|pair| MetricLabelPair::new(pair.name.to_owned(), pair.value.to_owned())),
        )
    }
}
impl<V> FromIterator<MetricLabelPair<V>> for MetricLabels
where
    V: Into<MetricString>,
{
    fn from_iter<I: IntoIterator<Item = MetricLabelPair<V>>>(iter: I) -> Self {
        Self::from_pairs(iter)
    }
}
impl PartialEq<krabka_blockstore::Labels> for MetricLabels {
    fn eq(&self, other: &krabka_blockstore::Labels) -> bool {
        self == &Self::from(other)
    }
}

/// Converts native input labels while retaining shared metric labels on WAL replay.
pub trait IntoMetricLabelsArc {
    fn into_metric_labels_arc(self) -> std::sync::Arc<MetricLabels>;
}
impl IntoMetricLabelsArc for MetricLabels {
    fn into_metric_labels_arc(self) -> std::sync::Arc<MetricLabels> {
        std::sync::Arc::new(self)
    }
}
impl IntoMetricLabelsArc for std::sync::Arc<MetricLabels> {
    fn into_metric_labels_arc(self) -> std::sync::Arc<MetricLabels> {
        self
    }
}
impl IntoMetricLabelsArc for krabka_blockstore::Labels {
    fn into_metric_labels_arc(self) -> std::sync::Arc<MetricLabels> {
        std::sync::Arc::new(self.into())
    }
}
impl IntoMetricLabelsArc for std::sync::Arc<krabka_blockstore::Labels> {
    fn into_metric_labels_arc(self) -> std::sync::Arc<MetricLabels> {
        std::sync::Arc::new(self.as_ref().into())
    }
}

impl From<BTreeMap<String, String>> for MetricLabels {
    fn from(labels: BTreeMap<String, String>) -> Self {
        let mut metric_labels = Self::new();
        for pair in labels {
            metric_labels.insert(pair.0, pair.1);
        }
        metric_labels
    }
}
impl From<BTreeMap<String, MetricString>> for MetricLabels {
    fn from(labels: BTreeMap<String, MetricString>) -> Self {
        Self(labels)
    }
}
#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    struct LabelFixture {
        pairs: Vec<MetricLabelPair<&'static str>>,
        expected: serde_json::Value,
        fingerprint: u64,
    }

    struct StringBuffers {
        name: String,
        name_pointer: *const u8,
        value_pointer: *const u8,
        has_value: bool,
    }

    #[test]
    fn owned_storage_labels_keep_values_wire_identity_and_string_buffers() {
        for fixture in [
            LabelFixture {
                pairs: vec![],
                expected: serde_json::json!({}),
                fingerprint: 0xcbf2_9ce4_8422_2325,
            },
            LabelFixture {
                pairs: vec![
                    MetricLabelPair::new("z", "discard"),
                    MetricLabelPair::new("a", "first"),
                    MetricLabelPair::new("z", "last"),
                ],
                expected: serde_json::json!({"a": "first", "z": "last"}),
                fingerprint: 0x787e_8a04_6823_a559,
            },
            LabelFixture {
                pairs: vec![MetricLabelPair::new("", "")],
                expected: serde_json::json!({"": ""}),
                fingerprint: 0x8820_1fb9_60ff_6465,
            },
            LabelFixture {
                pairs: vec![
                    MetricLabelPair::new("region", "東京"),
                    MetricLabelPair::new("α", "snow ☃"),
                ],
                expected: serde_json::json!({"region": "東京", "α": "snow ☃"}),
                fingerprint: 0xdaf3_0222_8f67_fbd8,
            },
            LabelFixture {
                pairs: vec![MetricLabelPair::new("a\0", "v\0x")],
                expected: serde_json::json!({"a\0": "v\0x"}),
                fingerprint: 0x0a08_c93e_4600_3999,
            },
            LabelFixture {
                pairs: vec![
                    MetricLabelPair::new("a=b", "c\nd"),
                    MetricLabelPair::new("x", "y=z\n"),
                ],
                expected: serde_json::json!({"a=b": "c\nd", "x": "y=z\n"}),
                fingerprint: 0xdf65_4ec5_30f1_30f1,
            },
        ] {
            let storage = krabka_blockstore::Labels::from_pairs(
                fixture
                    .pairs
                    .into_iter()
                    .map(|pair| krabka_blockstore::LabelPair::new(pair.name, pair.value)),
            );
            assert!(storage.fingerprint() == fixture.fingerprint);
            let buffers = storage
                .iter()
                .map(|pair| StringBuffers {
                    name: pair.name.to_owned(),
                    name_pointer: pair.name.as_ptr(),
                    value_pointer: pair.value.as_ptr(),
                    has_value: !pair.value.is_empty(),
                })
                .collect::<Vec<_>>();
            let borrowed = MetricLabels::from(&storage);
            let owned = MetricLabels::from(storage);
            assert!(owned == borrowed);
            assert!(serde_json::to_value(&owned).unwrap() == fixture.expected);
            assert!(owned.fingerprint() == fixture.fingerprint);
            for buffer in buffers {
                let pair = owned.iter().find(|pair| *pair.name == buffer.name).unwrap();
                let actual_name = pair.name;
                let value = pair.value;
                assert!(value.as_bytes() == value.as_str().as_bytes());
                if !buffer.name.is_empty() {
                    assert!(actual_name.as_ptr() == buffer.name_pointer);
                }
                if buffer.has_value {
                    assert!(value.as_str().as_ptr() == buffer.value_pointer);
                }
            }
        }
    }
}
