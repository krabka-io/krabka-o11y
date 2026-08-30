use super::{BlockDescriptor, ByteSize};

pub(crate) fn planned_block_bytes_for_blocks(blocks: &[BlockDescriptor]) -> ByteSize {
    blocks.iter().map(|block| block.size).sum()
}
