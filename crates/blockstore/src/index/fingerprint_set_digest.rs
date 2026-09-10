use super::SeriesFingerprint;

/// An order-independent digest of a block's fingerprint set.
///
/// A caller re-registers a block object key often. A manifest is re-applied,
/// or a writer restates the block it just wrote, and the series set is almost
/// always the same one. To recognise that costs one comparison here, and it
/// saves a rewrite of the inverted posting list, which is the only structure
/// that still holds the block-to-series pairs.
///
/// Each fingerprint goes through the `SplitMix64` finaliser before the sum, so
/// the digest does not collapse the way a bare XOR does. Two sets with
/// `a ^ b == c ^ d` are easy to build by accident. A collision here needs two
/// sets whose mixed sums agree in all 64 bits. The caller compares the element
/// count alongside the digest.
pub(crate) fn fingerprint_set_digest(
    fingerprints: impl IntoIterator<Item = SeriesFingerprint>,
) -> u64 {
    let mut digest = 0_u64;
    for fingerprint in fingerprints {
        let mut mixed = fingerprint;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        digest = digest.wrapping_add(mixed ^ (mixed >> 31));
    }
    digest
}
