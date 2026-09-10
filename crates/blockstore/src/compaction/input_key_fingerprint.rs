/// Hashes a compaction's input keys into the value that names its output.
///
/// A separator is folded in after each key, so where one key ends and the next
/// begins is part of the input: two different input sets must not agree on an
/// output name.
#[must_use]
pub fn input_key_fingerprint(input_keys: &[String]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for key in input_keys {
        for byte in key.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(PRIME);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
