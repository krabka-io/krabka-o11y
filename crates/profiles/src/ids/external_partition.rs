use super::*;

/// A partition key in the composite cold-read address space: a per-block base
/// OR-ed with a dense local id. Symbol resolution routes on this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
pub struct ExternalPartition(pub u64);
