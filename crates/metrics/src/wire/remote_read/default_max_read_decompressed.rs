use super::*;

/// Default decompressed-body cap for a `remote_read` request when the caller
/// supplies no cap. It mirrors the distributor's ingest default, so a single
/// `read` request cannot decompress to an unbounded allocation.
pub const DEFAULT_MAX_READ_DECOMPRESSED: ByteSize = mebibytes(32);
