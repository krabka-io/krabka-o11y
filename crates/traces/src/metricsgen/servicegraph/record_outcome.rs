/// Result of recording one span into the edge store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordOutcome {
    Recorded,
    Completed,
    Dropped,
    Ignored,
}
