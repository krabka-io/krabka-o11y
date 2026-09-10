/// Most grid slots one record may be written into before it is widened.
///
/// A record belongs to every slot its own span crosses, so a record whose span
/// is pathological -- a block whose timestamps run from the epoch to now, which
/// one bad clock produces -- would otherwise be written into millions of
/// shards. Past this many slots the record goes into the tenant's one
/// unbounded shard instead, which every load reads and every merge fetches.
/// That is a worse place to be, and it is where a record that has lost its
/// time bounds belongs.
pub(crate) const MAX_SHARD_SLOTS_PER_RECORD: i64 = 32;
