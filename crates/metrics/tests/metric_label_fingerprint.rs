use assert2::assert;
use krabka_metrics::MetricLabels;

#[test]
fn canonical_fingerprints_preserve_lengths_order_and_original_bytes() {
    let cases = [
        (vec![], 0xcbf2_9ce4_8422_2325),
        (
            vec![
                ("__name__".to_owned(), b"http_requests_total".to_vec()),
                ("job".to_owned(), b"api".to_vec()),
            ],
            0x24b4_e51d_5c88_a37e,
        ),
        (
            vec![("a=b\nc".to_owned(), b"d".to_vec())],
            0xf9e6_814e_652f_737e,
        ),
        (
            vec![("a".to_owned(), b"b\nc=d".to_vec())],
            0xe8f8_1506_9c0b_fd66,
        ),
        (vec![("a".to_owned(), vec![])], 0xffd8_0a17_8d19_199f),
        (vec![(String::new(), b"a".to_vec())], 0xba8f_d8a2_3d5c_6cdf),
        (vec![("a".to_owned(), vec![0xff])], 0x41c8_72a7_2d0b_9833),
        (vec![("a".to_owned(), vec![0xfe])], 0x41c8_71a7_2d0b_9680),
        (
            vec![("a".to_owned(), "\u{fffd}".as_bytes().to_vec())],
            0x32be_48e1_4211_37cd,
        ),
        (
            vec![("a".to_owned(), vec![0, 0xff, 0])],
            0x14c9_3ade_5b89_58c3,
        ),
        (
            vec![("a".to_owned(), vec![b'x'; 256])],
            0x810b_765c_d7e3_76c4,
        ),
        (
            vec![("a".repeat(256), b"x".to_vec())],
            0x4af9_7d5c_59ad_d1a9,
        ),
        (
            vec![("é".to_owned(), "☃".as_bytes().to_vec())],
            0x9ccf_2c0b_8143_2adf,
        ),
    ];
    for (pairs, expected) in cases {
        let labels = MetricLabels::from_pairs(pairs.clone());
        assert!(labels.fingerprint() == expected);
        assert!(MetricLabels::from_pairs(pairs.into_iter().rev()).fingerprint() == expected);
        if let Ok(storage_labels) = labels.to_storage_labels() {
            assert!(storage_labels.fingerprint() == expected);
        }
    }
}
