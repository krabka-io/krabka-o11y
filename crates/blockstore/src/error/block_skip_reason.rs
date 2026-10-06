use krabka_domain_macros::EnumName;

/// Why a scan left one block out of the result it returned.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, EnumName)]
#[non_exhaustive]
pub enum BlockSkipReason {
    /// The index named a key the object store does not have.
    #[name(value = "missing")]
    Missing,

    /// The object is there, and is not a readable Parquet block.
    #[name(value = "corrupt")]
    Corrupt,
}
