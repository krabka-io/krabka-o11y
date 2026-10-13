use super::Labels;

pub(crate) fn fingerprint_labels(labels: &[crate::ProfileLabel]) -> u64 {
    let mut canonical = Labels::new();
    for label in labels {
        canonical.insert(label.name.clone(), label.value.clone());
    }
    canonical.fingerprint()
}

/// Metadata-series identity excludes the per-profile association label.
/// Keep the full storage fingerprint for scans, statistics and profile lookup.
pub(crate) fn queried_series_fingerprint(labels: &[crate::ProfileLabel]) -> u64 {
    let mut canonical = Labels::new();
    for label in labels {
        if label.name != "__profile_id__" {
            canonical.insert(label.name.clone(), label.value.clone());
        }
    }
    canonical.fingerprint()
}

#[cfg(test)]
mod tests {
    use crate::in_memory::InMemoryProfileStore;

    #[test]
    fn pushed_sample_keeps_label_payload_and_canonical_fingerprint() {
        let mut store = InMemoryProfileStore::new();
        let labels = vec![
            crate::ProfileLabel::new("b", "old"),
            crate::ProfileLabel::new("a", "=x\n"),
            crate::ProfileLabel::new("b", "last"),
        ];
        store.push_sample_with_total_and_associations(
            crate::ProfileIdentity::new("tenant", "cpu"),
            labels,
            crate::StacktraceIdentity::new(17, 19),
            crate::SampleValues::new(7, 11),
            42,
            crate::SampleAssociations::new(Some(23), Some(vec![0, 255])),
        );
        assert2::assert!(store.samples.len() == 1 && store.samples["tenant"].len() == 1);
        let row = &store.samples["tenant"][0];
        assert2::assert!(row.profile_type == "cpu");
        assert2::assert!(row.fingerprint == 0xd1e5_33a9_4f60_f896);
        assert2::assert!(
            row.labels
                == vec![
                    crate::ProfileLabel::new("b", "old"),
                    crate::ProfileLabel::new("a", "=x\n"),
                    crate::ProfileLabel::new("b", "last"),
                ]
        );
        assert2::assert!(row.partition == 17 && row.stacktrace_id == 19);
        assert2::assert!(row.value == 7 && row.total_value == 11);
        assert2::assert!(row.span_id == Some(23));
        assert2::assert!(row.trace_id == Some(vec![0, 255]));
        assert2::assert!(row.timestamp_ms == 42);
    }
}
