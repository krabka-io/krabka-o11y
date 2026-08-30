use super::{BlockDescriptor, LokiDirection};

pub(crate) fn object_store_stream_blocks_in_scan_order(
    blocks: &[BlockDescriptor],
    direction: LokiDirection,
) -> Vec<&BlockDescriptor> {
    let mut blocks = blocks.iter().collect::<Vec<_>>();
    match direction {
        LokiDirection::Forward => {
            blocks.sort_by_key(|block| {
                (
                    block.key.time_range.start_ns,
                    block.key.time_range.end_ns,
                    block.key.partition,
                    block.key.first_offset,
                )
            });
        }
        LokiDirection::Backward => {
            blocks.sort_by_key(|block| {
                (
                    std::cmp::Reverse(block.key.time_range.end_ns),
                    std::cmp::Reverse(block.key.time_range.start_ns),
                    std::cmp::Reverse(block.key.partition),
                    std::cmp::Reverse(block.key.last_offset),
                )
            });
        }
    }
    blocks
}
