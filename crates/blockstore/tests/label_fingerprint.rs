//! Fixed canonical label fingerprints for owned and borrowed inputs.

use krabka_blockstore::Labels;

#[test]
fn fingerprints_preserve_order_duplicates_byte_lengths_and_delimiters() {
    // Independent vectors for FNV-1a over little-endian u64 byte lengths.
    let cases: &[(&[(&str, &str)], u64)] = &[
        (&[], 0xcbf2_9ce4_8422_2325),
        (&[("app", "api")], 0x9f4c_9766_3759_ad5a),
        (
            &[("b", "old"), ("a", "=x\n"), ("b", "last")],
            0xd1e5_33a9_4f60_f896,
        ),
        (
            &[("é", "🦀\0"), ("", ""), ("x", "y=z\n")],
            0x56a3_88ef_237b_8646,
        ),
        (&[("a", "b=c")], 0x3db5_b5df_0466_0932),
        (&[("a=b", "c")], 0x55db_7a79_d51d_8b94),
    ];
    for &(pairs, expected) in cases {
        assert2::assert!(Labels::fingerprint_pairs(pairs.iter().copied()) == expected);
        assert2::assert!(Labels::from_pairs(pairs.iter().copied()).fingerprint() == expected);
    }
}
