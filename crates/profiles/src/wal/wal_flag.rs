use super::*;

/// A wire-compatible boolean flag that [`WalMapping`] uses.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WalFlag(pub(crate) bool);

impl WalFlag {
    /// Return the contained flag value.
    #[must_use]
    pub const fn get(self) -> bool {
        self.0
    }
}

impl From<bool> for WalFlag {
    fn from(value: bool) -> Self {
        Self(value)
    }
}
