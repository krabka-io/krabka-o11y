/// One object-store operation, as the `operation` label spells it.
///
/// The enum is closed on purpose. It is the `operation` label's whole domain,
/// so the label cannot grow a value that no dashboard expects and no alert
/// covers. The variants are the methods the
/// [`ObjectStore`](object_store::ObjectStore) trait requires an implementation
/// to supply. Every other method on the trait has a default body that calls
/// one of these, so `head` is counted as `get` and `delete` is counted as
/// `delete_stream`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectStoreOperation {
    /// A single-request upload.
    Put,
    /// A multipart upload, counted when the upload starts.
    PutMultipart,
    /// A read, which covers `head`, `get_range` and `get_ranges`.
    Get,
    /// A flat listing.
    List,
    /// A listing that stops at the delimiter.
    ListWithDelimiter,
    /// A server-side copy, which covers `rename`.
    Copy,
    /// A bulk delete, which covers the single-object `delete`.
    DeleteStream,
    /// One whole Parquet block write, which
    /// [`BlockWriter`](crate::BlockWriter) retries as a unit.
    ///
    /// This is not a method on the
    /// [`ObjectStore`](object_store::ObjectStore) trait. Underneath it is one
    /// `put`, or a `put_multipart` and its parts, and those are counted under
    /// their own label as well. It has a label of its own because the block
    /// write is the unit the writer retries, so it is the unit a retry counter
    /// has to name.
    WriteBlock,
}

impl ObjectStoreOperation {
    /// The label value for this operation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::PutMultipart => "put_multipart",
            Self::Get => "get",
            Self::List => "list",
            Self::ListWithDelimiter => "list_with_delimiter",
            Self::Copy => "copy",
            Self::DeleteStream => "delete_stream",
            Self::WriteBlock => "write_block",
        }
    }

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

impl std::fmt::Display for ObjectStoreOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}
