use super::*;

/// Columns used to summarize a block's time bounds and distinct identity keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryColumns {
    pub id_col: String,
    pub ts_col: String,
}

impl SummaryColumns {
    #[must_use]
    pub fn new(id_col: impl Into<String>, ts_col: impl Into<String>) -> Self {
        Self {
            id_col: id_col.into(),
            ts_col: ts_col.into(),
        }
    }

    #[must_use]
    pub fn series() -> Self {
        Self::new(COL_FINGERPRINT, COL_TIMESTAMP)
    }
}
