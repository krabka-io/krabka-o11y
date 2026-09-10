/// The path spelling of one shard bound.
///
/// The bound is written in offset binary, with the sign bit flipped, then
/// padded with zeros to twenty digits. Twenty is the widest decimal a `u64`
/// takes. Both halves matter. The flip puts a negative timestamp below a
/// positive one instead of above it. The padding makes the lexicographic order
/// of the keys equal to the numeric order of the bounds. A listing over the
/// prefix therefore returns the shards in time order, which is the property
/// [`crate::index_snapshot`] gets from the same padding on its generation
/// numbers.
pub(crate) fn shard_bound_key(bound: i64) -> String {
    let ordered = u64::from_le_bytes(bound.to_le_bytes()) ^ (1 << 63);
    format!("{ordered:020}")
}
