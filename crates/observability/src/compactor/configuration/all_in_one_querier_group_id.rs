/// The consumer group an all-in-one querier tails the WAL under.
///
/// The block builder keeps the configured group. The querier gets its own,
/// because a consumer group hands each member a disjoint set of partitions:
/// two members of one group would leave the block builder writing blocks from
/// half the WAL and the querier tailing the other half, and neither would say
/// so. Deriving the name rather than adding a flag keeps the all-in-one
/// configured by exactly the keys the single-role files already use.
pub(crate) fn all_in_one_querier_group_id(wal_group_id: &str) -> String {
    format!("{wal_group_id}-querier")
}
