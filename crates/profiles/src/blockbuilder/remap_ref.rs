use super::*;

pub(crate) fn remap_ref(reference: u32, table: &[u32]) -> u32 {
    usize::try_from(reference)
        .ok()
        .and_then(|idx| table.get(idx))
        .copied()
        .or_else(|| {
            reference
                .checked_sub(1)
                .and_then(|idx| usize::try_from(idx).ok())
                .and_then(|idx| table.get(idx))
                .copied()
        })
        .unwrap_or(reference)
}
