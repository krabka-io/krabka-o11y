use super::{ByteSize, gibibytes};

/// Maximum on-disk byte size of a Parquet block accepted by [`read_block`].
///
/// Blocks come from shared object storage and, per the threat model, may be
/// corrupt or maliciously oversized. A stream of an unbounded Parquet file
/// could OOM the process. The reader `head()`s the block first and rejects it
/// above this cap. This mirrors the profiles gunzip `max_decompressed` output
/// cap. The default is 1 GiB, well above a realistic compacted block.
pub const DEFAULT_BLOCK_READ_MAX: ByteSize = gibibytes(1);
