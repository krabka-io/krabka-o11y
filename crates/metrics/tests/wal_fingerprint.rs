use assert2::assert;
use krabka_metrics::{MetricLabelPair, MetricLabels, SamplePayload, WalRecord};

#[test]
fn wal_fingerprints_keep_canonical_order_and_original_label_bytes() {
    let mut cases = canonical_cases();
    cases.extend([
        case(
            vec![label("a", b"old".to_vec()), label("a", vec![0xff])],
            0x41c8_72a7_2d0b_9833,
        ),
        case(
            vec![
                label("z", b"last".to_vec()),
                label("a", b"old".to_vec()),
                label("a", vec![0xfe]),
            ],
            0xf264_0131_96a1_a379,
        ),
        case(
            vec![
                label("a", b"old".to_vec()),
                label("z", b"last".to_vec()),
                label("a", "\u{fffd}".as_bytes().to_vec()),
            ],
            0xe399_2f90_611a_8c2a,
        ),
    ]);
    for case in cases {
        let record = record(case.pairs.clone());
        assert!(record.series_fingerprint() == case.expected);
        assert!(record.labels().fingerprint() == case.expected);
        let restored = WalRecord::decode(&record.encode().unwrap()).unwrap();
        assert!(restored == record && restored.series_fingerprint() == case.expected);
    }

    let mut reversed = record(vec![
        label("job", b"api".to_vec()),
        label("__name__", b"http_requests_total".to_vec()),
    ]);
    assert!(reversed.series_fingerprint() == 0x24b4_e51d_5c88_a37e);
    reversed.labels.reverse();
    assert!(reversed.series_fingerprint() == 0x24b4_e51d_5c88_a37e);
}

fn label(name: impl Into<String>, value: Vec<u8>) -> MetricLabelPair<Vec<u8>> {
    MetricLabelPair::new(name, value)
}

fn record(labels: Vec<MetricLabelPair<Vec<u8>>>) -> WalRecord {
    WalRecord {
        tenant: "tenant".to_owned(),
        labels: labels
            .into_iter()
            .map(|pair| MetricLabelPair::new(pair.name, pair.value.into()))
            .collect(),
        payload: SamplePayload::Float {
            timestamp_ms: 17,
            value: 3.5,
            start_timestamp_ms: Some(11),
        },
        exemplars: Vec::new(),
    }
}

#[test]
fn canonical_fingerprints_preserve_lengths_order_and_original_bytes() {
    for case in canonical_cases() {
        let labels = MetricLabels::from_pairs(case.pairs.clone());
        assert!(labels.fingerprint() == case.expected);
        assert!(
            MetricLabels::from_pairs(case.pairs.into_iter().rev()).fingerprint() == case.expected
        );
        if let Ok(storage_labels) = labels.to_storage_labels() {
            assert!(storage_labels.fingerprint() == case.expected);
        }
    }
}

struct FingerprintCase {
    pairs: Vec<MetricLabelPair<Vec<u8>>>,
    expected: u64,
}

fn case(pairs: Vec<MetricLabelPair<Vec<u8>>>, expected: u64) -> FingerprintCase {
    FingerprintCase { pairs, expected }
}

// Distinct label sets and the fingerprints they hash to: lengths, order,
// separators, empty names and values, and bytes that are not UTF-8.
fn canonical_cases() -> Vec<FingerprintCase> {
    vec![
        case(vec![], 0xcbf2_9ce4_8422_2325),
        case(
            vec![
                label("__name__", b"http_requests_total".to_vec()),
                label("job", b"api".to_vec()),
            ],
            0x24b4_e51d_5c88_a37e,
        ),
        case(vec![label("a=b\nc", b"d".to_vec())], 0xf9e6_814e_652f_737e),
        case(vec![label("a", b"b\nc=d".to_vec())], 0xe8f8_1506_9c0b_fd66),
        case(vec![label("a", vec![])], 0xffd8_0a17_8d19_199f),
        case(
            vec![label(String::new(), b"a".to_vec())],
            0xba8f_d8a2_3d5c_6cdf,
        ),
        case(vec![label("a", vec![0xff])], 0x41c8_72a7_2d0b_9833),
        case(vec![label("a", vec![0xfe])], 0x41c8_71a7_2d0b_9680),
        case(
            vec![label("a", "\u{fffd}".as_bytes().to_vec())],
            0x32be_48e1_4211_37cd,
        ),
        case(vec![label("a", vec![0, 0xff, 0])], 0x14c9_3ade_5b89_58c3),
        case(vec![label("a", vec![b'x'; 256])], 0x810b_765c_d7e3_76c4),
        case(
            vec![label("a".repeat(256), b"x".to_vec())],
            0x4af9_7d5c_59ad_d1a9,
        ),
        case(
            vec![label("é", "☃".as_bytes().to_vec())],
            0x9ccf_2c0b_8143_2adf,
        ),
    ]
}
