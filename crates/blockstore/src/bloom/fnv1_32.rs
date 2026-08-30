/// FNV-1 32-bit hash.
#[must_use]
pub fn fnv1_32(bytes: &[u8]) -> u32 {
    const OFFSET: u32 = 2_166_136_261;
    const PRIME: u32 = 16_777_619;

    let mut hash = OFFSET;
    for &b in bytes {
        hash = hash.wrapping_mul(PRIME);
        hash ^= u32::from(b);
    }
    hash
}
