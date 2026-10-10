use crate::{MetricStore, PromqlLabels as Labels};

/// The cardinality a [`MetricStore`] should report for `tenant-a`.
pub(crate) struct ExpectedCardinality<'a> {
    /// The active series, ordered by their `instance` label.
    pub(crate) active_series: Vec<Labels>,
    /// `(label name, series count)`, in the store's order.
    pub(crate) label_name_counts: &'a [(&'a str, usize)],
    /// `(label name, label value, series count)`, in the store's order.
    pub(crate) label_value_counts: &'a [(&'a str, &'a str, usize)],
}

/// Checks the active series and the label-name and label-value cardinality
/// that `store` reports for `tenant-a`.
pub(crate) async fn assert_tenant_cardinality<S: MetricStore>(
    store: &S,
    expected: ExpectedCardinality<'_>,
) {
    let mut active_series = store.cardinality_active_series("tenant-a").await.unwrap();
    active_series.sort_by_key(|labels| labels.get("instance").unwrap_or("").to_string());
    assert2::assert!(active_series == expected.active_series);

    let label_names = store.cardinality_label_names("tenant-a").await.unwrap();
    let name_counts = label_names
        .iter()
        .map(|stat| (stat.name.as_str(), stat.series_count))
        .collect::<Vec<_>>();
    assert2::assert!(name_counts == expected.label_name_counts);

    let label_values = store.cardinality_label_values("tenant-a").await.unwrap();
    let value_counts = label_values
        .iter()
        .map(|stat| {
            (
                stat.label_name.as_str(),
                stat.label_value.as_str(),
                stat.series_count,
            )
        })
        .collect::<Vec<_>>();
    assert2::assert!(value_counts == expected.label_value_counts);
}
