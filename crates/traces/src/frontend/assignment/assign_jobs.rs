use super::{AssignedJob, JobShard, rendezvous_pick, shard_key};

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
/// rotation -- see [`rendezvous_pick`].
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
