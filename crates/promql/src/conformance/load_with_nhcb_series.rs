use super::{BTreeMap, Line, LoadSeries, NhcbBucketSeries, NhcbGroup, Result, conformance_labels_key, labels_to_metric, labels_without_label, nhcb_sample_at, parse_bucket_bound, testkit};

pub(crate) fn load_with_nhcb_series(series: &[LoadSeries], line: Line<'_>) -> Result<Vec<LoadSeries>> {
    let mut groups: BTreeMap<String, NhcbGroup> = BTreeMap::new();
    for load_series in series {
        let labels = testkit::metric_to_labels(&load_series.metric);
        let Some(name) = labels.get("__name__") else {
            continue;
        };
        if let Some(native_name) = name.strip_suffix("_sum") {
            let mut native_labels = labels.clone();
            native_labels.insert("__name__", native_name);
            let key = conformance_labels_key(&native_labels);
            groups
                .entry(key)
                .or_insert_with(|| NhcbGroup {
                    labels: native_labels,
                    buckets: Vec::new(),
                    sum_values: None,
                })
                .sum_values = Some(load_series.values.clone());
            continue;
        }
        let Some(native_name) = name.strip_suffix("_bucket") else {
            continue;
        };
        let Some(le) = labels.get("le") else {
            continue;
        };
        let upper_bound = parse_bucket_bound(le, line)?;
        let mut native_labels = labels_without_label(&labels, "le");
        native_labels.insert("__name__", native_name);
        let key = conformance_labels_key(&native_labels);
        groups
            .entry(key)
            .or_insert_with(|| NhcbGroup {
                labels: native_labels,
                buckets: Vec::new(),
                sum_values: None,
            })
            .buckets
            .push(NhcbBucketSeries {
                upper_bound,
                values: load_series.values.clone(),
            });
    }

    let mut out = Vec::new();
    for mut group in groups.into_values() {
        group
            .buckets
            .sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));
        if group.buckets.is_empty() {
            continue;
        }
        let sample_count = group
            .buckets
            .iter()
            .map(|bucket| bucket.values.len())
            .max()
            .unwrap_or(0);
        let custom_values = group
            .buckets
            .iter()
            .filter_map(|bucket| bucket.upper_bound.is_finite().then_some(bucket.upper_bound))
            .collect::<Vec<_>>();
        let mut values = Vec::with_capacity(sample_count);
        for index in 0..sample_count {
            values.push(nhcb_sample_at(
                &group.buckets,
                group.sum_values.as_deref(),
                &custom_values,
                index,
                line,
            )?);
        }
        out.push(LoadSeries {
            metric: labels_to_metric(&group.labels),
            values,
        });
    }
    Ok(out)
}
