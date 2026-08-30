pub(crate) fn remap_index(index: u32, remapped: &[u32]) -> u32 {
    remapped
        .get(usize::try_from(index).expect("u32 fits usize"))
        .copied()
        .unwrap_or(index)
}
