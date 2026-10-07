use std::collections::BTreeMap;

use crate::MetricString;

/// Query labels with Go string byte values, converted from UTF-8 storage labels.
#[derive(
    Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MetricLabels(BTreeMap<String, MetricString>);

impl MetricLabels {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<MetricString>,
    {
        Self(
            pairs
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        )
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
    pub fn iter(&self) -> impl Iterator<Item = (&String, &MetricString)> {
        self.0.iter()
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
            .map(|(name, value)| {
                let value = value
                    .utf8()
                    .ok_or_else(|| "metric storage requires UTF-8 label values".to_owned())?;
                Ok((name.clone(), value.to_owned()))
            })
            .collect()
    }
    /// Returns only UTF-8 label values for the generic posting index.
    /// The caller retains this metric label set as the authoritative identity.
    #[must_use]
    pub fn utf8_projection(&self) -> krabka_blockstore::Labels {
        self.iter()
            .filter_map(|(name, value)| value.utf8().map(|value| (name.clone(), value.to_owned())))
            .collect()
    }
    #[must_use]
    pub fn has_byte_values(&self) -> bool {
        self.iter().any(|(_, value)| value.utf8().is_none())
    }
    /// Canonical length-prefixed byte identity, shared by collision and matching keys.
    #[must_use]
    pub fn byte_key(&self) -> Vec<u8> {
        let capacity = self.iter().fold(0usize, |capacity, (name, value)| {
            capacity
                .saturating_add(16)
                .saturating_add(name.len())
                .saturating_add(value.as_bytes().len())
        });
        let mut key = Vec::with_capacity(capacity);
        for (name, value) in self.iter() {
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
        for (name, value) in self.iter() {
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
        self.byte_key()
            .into_iter()
            .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    }
}
impl From<krabka_blockstore::Labels> for MetricLabels {
    fn from(labels: krabka_blockstore::Labels) -> Self {
        Self::from(&labels)
    }
}
impl From<&krabka_blockstore::Labels> for MetricLabels {
    fn from(labels: &krabka_blockstore::Labels) -> Self {
        Self::from_pairs(
            labels
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
    }
}
impl<K, V> FromIterator<(K, V)> for MetricLabels
where
    K: Into<String>,
    V: Into<MetricString>,
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
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
        Self::from_pairs(labels)
    }
}
impl From<BTreeMap<String, MetricString>> for MetricLabels {
    fn from(labels: BTreeMap<String, MetricString>) -> Self {
        Self(labels)
    }
}
impl IntoIterator for MetricLabels {
    type Item = (String, MetricString);
    type IntoIter = std::collections::btree_map::IntoIter<String, MetricString>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
impl<'a> IntoIterator for &'a MetricLabels {
    type Item = (&'a String, &'a MetricString);
    type IntoIter = std::collections::btree_map::Iter<'a, String, MetricString>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}
