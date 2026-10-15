//! Which querier scans which shard.
//!
//! Round-robin is a load-balancing decision. Whether that is the right
//! decision depends on whether queriers are interchangeable, and in this stack
//! they are interchangeable for one shard kind and genuinely sharded for the
//! other. [`assign_jobs`] carries the evidence for that split and acts on it.

use crate::frontend::job::JobShard;

/// One planned shard, and the querier that will scan it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssignedJob {
    /// What to scan.
    pub shard: JobShard,
    /// The `host:port` to scan it on.
    pub querier: String,
}

/// The score of `key` on `addr`, for rendezvous hashing.
///
/// FNV-1a over `key \x1f addr`, then a `splitmix64` finalizer. FNV-1a rather
/// than `DefaultHasher` because the assignment has to be the same in two
/// processes and across two builds: `DefaultHasher`'s algorithm is explicitly
/// not guaranteed stable, and a frontend that hashes differently after an
/// upgrade re-shuffles every block to a different querier for no reason.
///
/// The finalizer is not decoration. Pool addresses differ from one another in
/// a byte or two, and FNV-1a's avalanche over a short common suffix is poor
/// enough that the highest-scoring querier is not evenly distributed -- a
/// four-querier pool took a 42% share of 400 blocks without it. Mixing the
/// output restores an even spread, which is what makes an ownership hash
/// double as a balancer.
pub(crate) fn rendezvous_score(addr: &str, key: &str) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for byte in key.as_bytes().iter().chain(b"\x1f").chain(addr.as_bytes()) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}


/// Place every planned shard on a ready querier.
///
/// The two shard kinds are not the same problem, and conflating them is what
/// made a single round-robin counter look sufficient.
///
/// **Cold-block shards are interchangeable work.** The shard names an object
/// key and a row-group range, every querier loads the same `TraceIndex`
/// snapshot from the same bucket, and every querier reads blocks out of that
/// bucket. Any ready querier gives byte-identical results for a block job. The
/// only thing to decide is which one, and the answer is ownership rather than
/// rotation -- see `rendezvous_pick`.
///
/// **The live shard is not interchangeable.** A querier run with
/// `--querier-live-store` holds its own hot tier, fed by a WAL consumer in the
/// group `krabka-traces-querier-live-store`; N querier replicas in one group
/// are assigned disjoint partitions, so each holds a *different* slice of the
/// recent spans and no single querier holds them all. Sending the live shard
/// to one querier chosen by a counter therefore answers from roughly 1/N of
/// the hot tier and returns it as a complete 200. The frontend cannot narrow a
/// live job to the partitions one querier owns -- that assignment lives inside
/// the consumer group and is never reported over the query API -- so the only
/// correct fan-out is every ready querier, unioned.
///
/// The union is safe in the other topology too. Queriers pointed at a shared
/// live-store with `--querier-live-store-url` all return the same recent
/// spans, and the merge reunions by `traceID`, so the duplicates collapse.
/// What it costs there is a repeated scan, not a wrong answer. It is the same
/// reasoning `QueryFrontend::trace_by_id` already applies, for the same reason.
#[must_use]
pub fn assign_jobs(jobs: Vec<JobShard>, ready: &[&str]) -> Vec<AssignedJob> {
    let mut assigned = Vec::with_capacity(jobs.len());
    for shard in jobs {
        match shard {
            JobShard::Live => assigned.extend(ready.iter().map(|addr| AssignedJob {
                shard: JobShard::Live,
                querier: (*addr).to_string(),
            })),
            JobShard::Block { .. } => {
                let Some(querier) = rendezvous_pick(ready, &shard_key(&shard)) else {
                    continue;
                };
                assigned.push(AssignedJob {
                    shard,
                    querier: querier.to_string(),
                });
            }
        }
    }
    assigned
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn block(id: &str, start: u32, end: u32) -> JobShard {
        JobShard::Block {
            block_id: id.to_string(),
            row_group_start: start,
            row_group_end: end,
        }
    }

    fn queriers_for(assigned: &[AssignedJob]) -> Vec<&str> {
        assigned.iter().map(|a| a.querier.as_str()).collect()
    }

    #[test]
    fn the_live_shard_reaches_every_ready_querier_and_a_block_reaches_exactly_one() {
        let ready = ["a:1", "b:1", "c:1"];
        let assigned = assign_jobs(vec![JobShard::Live, block("blk", 0, 4)], &ready);

        let live: Vec<&str> = assigned
            .iter()
            .filter(|a| a.shard == JobShard::Live)
            .map(|a| a.querier.as_str())
            .collect();
        check!(
            live == vec!["a:1", "b:1", "c:1"],
            "the hot tier is sharded across queriers"
        );

        let cold: Vec<&AssignedJob> = assigned
            .iter()
            .filter(|a| a.shard != JobShard::Live)
            .collect();
        check!(cold.len() == 1, "a block is readable from any querier");
        check!(ready.contains(&cold[0].querier.as_str()));
    }

    #[test]
    fn a_block_keeps_its_querier_while_the_pool_is_unchanged() {
        let ready = ["a:1", "b:1", "c:1"];
        let first = assign_jobs(vec![block("blk", 0, 4)], &ready);
        let again = assign_jobs(vec![block("blk", 0, 4)], &ready);
        check!(first == again);

        // Probe order must not matter: the same set in another order assigns
        // the same way.
        let shuffled = ["c:1", "a:1", "b:1"];
        check!(assign_jobs(vec![block("blk", 0, 4)], &shuffled) == first);
    }

    #[test]
    fn losing_a_querier_moves_only_the_blocks_it_owned() {
        let ready = ["a:1", "b:1", "c:1"];
        let blocks: Vec<JobShard> = (0..64).map(|i| block(&format!("blk-{i}"), 0, 4)).collect();
        let before = assign_jobs(blocks.clone(), &ready);

        let survivors = ["a:1", "b:1"];
        let after = assign_jobs(blocks, &survivors);

        let moved = before
            .iter()
            .zip(&after)
            .filter(|pair| pair.0.querier != pair.1.querier)
            .count();
        let owned_by_c = before.iter().filter(|a| a.querier == "c:1").count();
        check!(owned_by_c > 0, "c owned some of the 64 blocks");
        check!(
            moved == owned_by_c,
            "only c's blocks move; a modulus would reshuffle nearly all of them"
        );
    }

    #[test]
    fn the_row_group_range_is_part_of_the_ownership_key() {
        // Two halves of one block are separate work, so they may land on
        // different queriers rather than both following the block id.
        let ready = ["a:1", "b:1", "c:1", "d:1"];
        let assigned = assign_jobs(vec![block("blk", 0, 4), block("blk", 4, 8)], &ready);
        check!(assigned.len() == 2);
        check!(shard_key(&assigned[0].shard) != shard_key(&assigned[1].shard));
    }

    #[test]
    fn no_ready_querier_assigns_no_work_at_all() {
        check!(assign_jobs(vec![JobShard::Live, block("blk", 0, 4)], &[]).is_empty());
    }

    #[test]
    fn the_spread_over_a_pool_is_even_enough_to_be_a_balancer() {
        let ready = ["a:1", "b:1", "c:1", "d:1"];
        let blocks: Vec<JobShard> = (0..400).map(|i| block(&format!("blk-{i}"), 0, 4)).collect();
        let assigned = assign_jobs(blocks, &ready);
        for addr in ready {
            let share = queriers_for(&assigned)
                .iter()
                .filter(|q| **q == addr)
                .count();
            check!((50..=150).contains(&share), "{addr} took {share} of 400");
        }
    }
}



/// The ready querier that owns `key`, for work that is not shard-planned.
///
/// A `TraceQL`-metrics query runs as one unsharded job, so it has no shard to
/// assign; it still needs a target, and it still must not be a querier the
/// membership has ejected. Keying on the query text keeps a repeated query on
/// the same querier while the pool holds, which is worth having for whatever
/// that querier has already decoded.
#[must_use]
pub fn pick_querier<'a>(ready: &[&'a str], key: &str) -> Option<&'a str> {
    rendezvous_pick(ready, key)
}

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

/// The ownership key of a cold-block shard.
///
/// It names the block and the row-group range, so the two halves of a split
/// block are owned independently and a large block's work spreads instead of
/// landing whole on one querier.
pub(crate) fn shard_key(shard: &JobShard) -> String {
    match shard {
        JobShard::Live => "live".to_string(),
        JobShard::Block {
            block_id,
            row_group_start,
            row_group_end,
        } => format!("{block_id}#{row_group_start}-{row_group_end}"),
    }
}
