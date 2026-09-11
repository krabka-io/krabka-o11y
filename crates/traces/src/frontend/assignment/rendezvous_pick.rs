use super::rendezvous_score;

/// Pick the querier that owns `key`, by highest random weight.
///
/// Rendezvous hashing, not round-robin. Round-robin is a load-balancing
/// decision; this is an ownership one, and ownership wants two properties
/// round-robin does not have. The same block goes to the same querier while
/// the pool is unchanged, so a querier's page cache and its decoded row-group
/// work are worth having. And when a querier leaves, only the keys it owned
/// move -- every other block keeps its owner, rather than the whole assignment
/// rotating by one the way a modulus does.
///
/// Ties break on the address so the choice is total and reproducible.
/// `None` only when `candidates` is empty.
pub(crate) fn rendezvous_pick<'a>(candidates: &[&'a str], key: &str) -> Option<&'a str> {
    candidates
        .iter()
        .max_by(|a, b| {
            rendezvous_score(a, key)
                .cmp(&rendezvous_score(b, key))
                .then_with(|| a.cmp(b))
        })
        .copied()
}
