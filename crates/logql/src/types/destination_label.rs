use super::*;

/// The extracted-field name an extraction writes into.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DestinationLabel(pub String);
