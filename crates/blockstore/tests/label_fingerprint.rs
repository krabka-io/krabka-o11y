//! Fixed canonical label fingerprints.

use krabka_blockstore::{LabelPair, Labels};

struct FingerprintCase {
    pairs: Vec<LabelPair>,
    expected: u64,
}

#[test]
fn fingerprints_preserve_order_duplicates_byte_lengths_and_delimiters() {
    // Independent vectors for FNV-1a over little-endian u64 byte lengths.
    let long_name = "n".repeat(257);
    let long_utf8_value = "🦀".repeat(129);
    let value_256 = "x".repeat(256);
    let value_257 = "y".repeat(257);
    let cases = [
        FingerprintCase {
            pairs: Vec::new(),
            expected: 0xcbf2_9ce4_8422_2325,
        },
        FingerprintCase {
            pairs: vec![LabelPair::new("app", "api")],
            expected: 0x9f4c_9766_3759_ad5a,
        },
        FingerprintCase {
            pairs: vec![
                LabelPair::new("b", "old"),
                LabelPair::new("a", "=x\n"),
                LabelPair::new("b", "last"),
            ],
            expected: 0xd1e5_33a9_4f60_f896,
        },
        FingerprintCase {
            pairs: vec![
                LabelPair::new("é", "🦀\0"),
                LabelPair::new("", ""),
                LabelPair::new("x", "y=z\n"),
            ],
            expected: 0x56a3_88ef_237b_8646,
        },
        FingerprintCase {
            pairs: vec![LabelPair::new("a", "b=c")],
            expected: 0x3db5_b5df_0466_0932,
        },
        FingerprintCase {
            pairs: vec![LabelPair::new("a=b", "c")],
            expected: 0x55db_7a79_d51d_8b94,
        },
        FingerprintCase {
            pairs: vec![LabelPair::new(long_name, long_utf8_value)],
            expected: 0xc7ac_5780_9d34_8f6e,
        },
        FingerprintCase {
            pairs: vec![
                LabelPair::new("", value_256),
                LabelPair::new("z", value_257),
            ],
            expected: 0x7e75_f99e_f624_30cc,
        },
    ];
    for case in cases {
        assert2::assert!(Labels::from_pairs(case.pairs).fingerprint() == case.expected);
    }
}
