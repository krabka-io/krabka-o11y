use super::{Display, From, Into};

/// The source field a `logfmt` extraction reads from before renaming.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct SourceLabel(pub String);
