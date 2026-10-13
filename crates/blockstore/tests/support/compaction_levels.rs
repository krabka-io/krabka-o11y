pub fn block_levels<T>(
    blocks: &[T],
    mut level_of: impl FnMut(&T) -> krabka_blockstore::BlockLevel,
) -> Vec<krabka_blockstore::BlockLevel> {
    blocks.iter().map(&mut level_of).collect()
}
