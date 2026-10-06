use krabka_domain_macros::EnumName;

/// One object-store operation, as the `operation` label spells it.
///
/// The enum is closed on purpose. It is the `operation` label's whole domain,
/// so the label cannot grow a value that no dashboard expects and no alert
/// covers. The variants are the methods the
/// [`ObjectStore`](object_store::ObjectStore) trait requires an implementation
/// to supply. Every other method on the trait has a default body that calls
/// one of these, so `head` is counted as `get` and `delete` is counted as
/// `delete_stream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumName)]
pub enum ObjectStoreOperation {
    /// A single-request upload.
    #[name(value = "put")]
    Put,
    /// A multipart request: open, part, complete, or abort.
    #[name(value = "put_multipart")]
    PutMultipart,
    /// A read, which covers `head`, `get_range` and `get_ranges`.
    #[name(value = "get")]
    Get,
    /// A flat listing.
    #[name(value = "list")]
    List,
    /// A listing that stops at the delimiter.
    #[name(value = "list_with_delimiter")]
    ListWithDelimiter,
    /// A server-side copy, which covers `rename`.
    #[name(value = "copy")]
    Copy,
    /// A bulk delete, which covers the single-object `delete`.
    #[name(value = "delete_stream")]
    DeleteStream,
    /// One whole Parquet block write, which
    /// [`BlockWriter`](crate::BlockWriter) retries as a unit.
    ///
    /// This is not a method on the
    /// [`ObjectStore`](object_store::ObjectStore) trait. Underneath it is one
    /// `put`, or multipart requests, and those are counted under their own
    /// labels as well. It has a label of its own because the block
    /// write is the unit the writer retries, so it is the unit a retry counter
    /// has to name.
    #[name(value = "write_block")]
    WriteBlock,
}

impl ObjectStoreOperation {
    /// Every operation, for a test that wants to walk the label domain.
    #[must_use]
    pub const fn all() -> [Self; 8] {
        [
            Self::Put,
            Self::PutMultipart,
            Self::Get,
            Self::List,
            Self::ListWithDelimiter,
            Self::Copy,
            Self::DeleteStream,
            Self::WriteBlock,
        ]
    }
}
