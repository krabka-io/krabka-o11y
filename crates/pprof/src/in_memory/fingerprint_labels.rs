use super::Labels;

pub(crate) fn fingerprint_labels(labels: &[(String, String)]) -> u64 {
    Labels::fingerprint_pairs(
        labels
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str())),
    )
}

#[cfg(test)]
mod tests {
    use crate::in_memory::InMemoryProfileStore;

    #[test]
    fn pushed_sample_keeps_label_payload_and_canonical_fingerprint() {
        let mut store = InMemoryProfileStore::new();
        let labels = vec![
            ("b".into(), "old".into()),
            ("a".into(), "=x\n".into()),
            ("b".into(), "last".into()),
        ];
        store.push_sample_with_total_and_associations(
            ("tenant", "cpu"),
            labels,
            (17, 19),
            (7, 11),
            42,
            (Some(23), Some(vec![0, 255])),
        );
        assert2::assert!(store.samples.len() == 1 && store.samples["tenant"].len() == 1);
        let row = &store.samples["tenant"][0];
        assert2::assert!(
            (
                &row.profile_type,
                row.fingerprint,
                &row.labels,
                row.partition,
                row.stacktrace_id,
                row.value,
                row.total_value,
                row.span_id,
                &row.trace_id,
                row.timestamp_ms
            ) == (
                &"cpu".to_string(),
                0xd1e5_33a9_4f60_f896,
                &vec![
                    ("b".into(), "old".into()),
                    ("a".into(), "=x\n".into()),
                    ("b".into(), "last".into())
                ],
                17,
                19,
                7,
                11,
                Some(23),
                &Some(vec![0, 255]),
                42
            )
        );
    }
}
