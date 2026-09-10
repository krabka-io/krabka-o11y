/// How often, in generations, a snapshot write also sweeps orphaned payloads.
///
/// The sweep lists the payload prefix and reads every retained manifest, which
/// is cheap against a flush that used to republish the whole index and not
/// cheap against one that now writes a single shard. Orphans are only wasted
/// storage, never wrong answers, so paying for the sweep on one generation in
/// sixteen is enough.
pub(crate) const SNAPSHOT_SWEEP_INTERVAL: u64 = 16;
