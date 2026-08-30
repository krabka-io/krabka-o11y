use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexSnapshotRetain(pub(crate) usize);

impl IndexSnapshotRetain {
    /// Validates an index-snapshot retention count.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` is zero.
    pub fn new(value: usize) -> std::result::Result<Self, String> {
        GreaterUsize::<0>::new(value)
            .map(|value| Self(value.into_value()))
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub const fn into_value(self) -> usize {
        self.0
    }
}

impl Default for IndexSnapshotRetain {
    fn default() -> Self {
        Self::new(DEFAULT_INDEX_SNAPSHOT_RETAIN)
            .expect("default index snapshot retention is positive")
    }
}

impl fmt::Display for IndexSnapshotRetain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for IndexSnapshotRetain {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        value
            .parse()
            .map_err(|error: std::num::ParseIntError| error.to_string())
            .and_then(Self::new)
    }
}
