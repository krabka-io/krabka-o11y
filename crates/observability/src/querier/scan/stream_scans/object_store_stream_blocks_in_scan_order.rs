use super::{BlockDescriptor, LokiDirection};

pub(crate) fn object_store_stream_blocks_in_scan_order(
    blocks: &[BlockDescriptor],
    direction: LokiDirection,
) -> Vec<&BlockDescriptor> {
    let mut blocks = blocks.iter().collect::<Vec<_>>();
    match direction {
        LokiDirection::Forward => {
            blocks.sort_by(|left, right| {
                left.key
                    .time_range
                    .start_ns
                    .cmp(&right.key.time_range.start_ns)
                    .then_with(|| left.key.time_range.end_ns.cmp(&right.key.time_range.end_ns))
                    .then_with(|| left.key.partition.cmp(&right.key.partition))
                    .then_with(|| left.key.first_offset.cmp(&right.key.first_offset))
            });
        }
        LokiDirection::Backward => {
            blocks.sort_by(|left, right| {
                right
                    .key
                    .time_range
                    .end_ns
                    .cmp(&left.key.time_range.end_ns)
                    .then_with(|| {
                        right
                            .key
                            .time_range
                            .start_ns
                            .cmp(&left.key.time_range.start_ns)
                    })
                    .then_with(|| right.key.partition.cmp(&left.key.partition))
                    .then_with(|| right.key.last_offset.cmp(&left.key.last_offset))
            });
        }
    }
    blocks
}
