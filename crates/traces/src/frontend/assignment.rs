//! Which querier scans which shard.
//!
//! Round-robin is a load-balancing decision. Whether that is the right
//! decision depends on whether queriers are interchangeable, and in this stack
//! they are interchangeable for one shard kind and genuinely sharded for the
//! other. [`assign_jobs`] carries the evidence for that split and acts on it.

use crate::frontend::job::JobShard;

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
            .filter(|(b, a)| b.querier != a.querier)
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

mod assign_jobs;
mod assigned_job;
mod pick_querier;
mod rendezvous_pick;
mod rendezvous_score;
mod shard_key;

pub use assign_jobs::assign_jobs;
pub use assigned_job::AssignedJob;
pub use pick_querier::pick_querier;
use rendezvous_pick::rendezvous_pick;
use rendezvous_score::rendezvous_score;
use shard_key::shard_key;
