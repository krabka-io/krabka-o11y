use super::{BTreeMap, Deserialize, Serialize, SeriesFingerprint};

/// An ordered set of `name -> value` labels identifying a series.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Labels(pub(crate) BTreeMap<String, String>);

impl Labels {
    #[must_use]
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Builds a label set from an iterator of `(name, value)` pairs.
    #[must_use]
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut labels = Self::new();
        for (name, value) in pairs {
            labels.insert(name, value);
        }
        labels
    }

    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.0.insert(name.into(), value.into());
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
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

    /// FNV-1a 64-bit hash over canonical length-prefixed `name` and `value`
    /// entries.
    ///
    /// Each name and value carries its byte length first (`u64`
    /// little-endian), so the encoding is injective. A name or value that
    /// contains `=` or a newline cannot be re-parsed across the field
    /// boundary. A bare `name=value\n` separator encoding would allow that,
    /// for example `a=b\nc` against `a` with value `b\nc`. Profile labels are
    /// user-controlled, so this collision is reachable, and the length prefix
    /// closes it. `BTreeMap` keeps names sorted, so the hash does not depend
    /// on insertion order. Krabka is greenfield, so no persisted fingerprint
    /// depends on the old encoding.
    #[must_use]
    pub fn fingerprint(&self) -> SeriesFingerprint {
        const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
        const PRIME: u64 = 0x0000_0100_0000_01b3;

        let mut hash = OFFSET;
        for (name, value) in &self.0 {
            for byte in (name.len() as u64)
                .to_le_bytes()
                .iter()
                .copied()
                .chain(name.as_bytes().iter().copied())
                .chain((value.len() as u64).to_le_bytes().iter().copied())
                .chain(value.as_bytes().iter().copied())
            {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(PRIME);
            }
        }
        hash
    }
}

impl FromIterator<(String, String)> for Labels {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}
