/// The zstd level every block is compressed at.
///
/// Blocks live on object storage and are read back over the network, so the
/// bytes are the cost and the codec is chosen for ratio per CPU second rather
/// than for raw decode speed. Level 1 is zstd's fastest setting and still
/// compresses a sorted metrics block several-fold; the levels above it buy a
/// few more percent for several times the write CPU, which a block builder
/// under ingest load cannot spend.
pub const BLOCK_ZSTD_LEVEL: i32 = 1;
