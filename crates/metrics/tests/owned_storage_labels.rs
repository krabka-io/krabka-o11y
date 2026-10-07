use krabka_blockstore::Labels as StorageLabels;
use krabka_metrics::MetricLabels;

#[test]
fn owned_storage_labels_preserve_the_ledger_and_string_buffers() {
    let cases = [
        (vec![], vec![]),
        (vec![("region", "東京")], vec![("region", "東京")]),
        (
            vec![("z", "last"), ("a", "é\0\n="), ("", ""), ("nul\0", "�")],
            vec![("", ""), ("a", "é\0\n="), ("nul\0", "�"), ("z", "last")],
        ),
        (
            vec![("app", "api"), ("env", "prod"), ("app", "web")],
            vec![("app", "web"), ("env", "prod")],
        ),
    ];
    for (input, expected) in cases {
        let storage = StorageLabels::from_pairs(input.iter().copied());
        let fingerprint = storage.fingerprint();
        let buffers = storage
            .iter()
            .map(|(name, value)| (name.as_ptr(), value.as_ptr()))
            .collect::<Vec<_>>();

        let converted = MetricLabels::from(storage);
        let ledger = converted
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_bytes(), value.as_str()))
            .collect::<Vec<_>>();
        let expected_ledger = expected
            .iter()
            .map(|(name, value)| (*name, value.as_bytes(), *value))
            .collect::<Vec<_>>();
        assert2::assert!(ledger == expected_ledger);
        assert2::assert!(converted.fingerprint() == fingerprint);

        let transferred_buffers = converted
            .iter()
            .map(|(name, value)| (name.as_ptr(), value.as_str().as_ptr()))
            .collect::<Vec<_>>();
        assert2::assert!(transferred_buffers == buffers);
    }
}
