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
