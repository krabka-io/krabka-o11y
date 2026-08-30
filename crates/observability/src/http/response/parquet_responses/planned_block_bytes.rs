use super::*;

pub(crate) fn planned_block_bytes(plan: &StreamPlan) -> ByteSize {
    planned_block_bytes_for_blocks(&plan.blocks)
}
