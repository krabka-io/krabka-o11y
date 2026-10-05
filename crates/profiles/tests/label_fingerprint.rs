//! Profiles WAL fingerprints use the canonical hash without rewriting labels.

use krabka_profiles::wal::{ProfileRecord, WalSymbolSet};

#[test]
fn wal_fingerprint_preserves_duplicate_labels_and_wire_payload() {
    let record = ProfileRecord {
        tenant: "tenant".into(),
        labels: vec![
            ("b".into(), "old".into()),
            ("a".into(), "=x\n".into()),
            ("b".into(), "last".into()),
        ],
        profile_type: "cpu".into(),
        samples: Vec::new(),
        symbols: WalSymbolSet {
            strings: vec![String::new()],
            functions: Vec::new(),
            locations: Vec::new(),
            mappings: Vec::new(),
        },
    };
    assert2::assert!(record.series_fingerprint() == 0xd1e5_33a9_4f60_f896);
    let decoded = ProfileRecord::decode(&record.encode().unwrap()).unwrap();
    assert2::assert!(decoded == record);
    assert2::assert!(decoded.series_fingerprint() == 0xd1e5_33a9_4f60_f896);
}
