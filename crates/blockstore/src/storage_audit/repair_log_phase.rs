use super::{Deserialize, Serialize};

/// Which of the two lines of a delete a repair log line is.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairLogPhase {
    /// The repair is about to delete the object. It wrote and synced this
    /// line before the delete started, so an intent with no outcome line
    /// after it marks an object that the repair may have deleted.
    Intent,
    /// What the repair did with the object.
    Outcome,
}
