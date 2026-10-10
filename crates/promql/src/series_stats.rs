//! Cardinality and TSDB-stat summaries shared by the metric store implementations.

use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::SeriesFingerprint;

use crate::{
    LabelNameCardinality, LabelValueCardinality, NamedTsdbStat, PromqlLabels as Labels,
    TsdbHeadStats, TsdbStats,
};

/// One series' fingerprint and labels.
#[derive(Clone, Copy)]
pub(crate) struct SeriesRef<'a> {
    pub(crate) fp: SeriesFingerprint,
    pub(crate) labels: &'a Labels,
}

/// Counts the distinct series carrying each label name, ordered by descending
/// series count, then by name.
pub(crate) fn label_name_cardinality<'a>(
    series: impl IntoIterator<Item = SeriesRef<'a>>,
) -> Vec<LabelNameCardinality> {
    let mut by_name = BTreeMap::<String, BTreeSet<SeriesFingerprint>>::new();
    for SeriesRef { fp, labels } in series {
        for (name, _) in labels.iter() {
            by_name.entry(name.clone()).or_default().insert(fp);
        }
    }
    let mut cardinality = by_name
        .into_iter()
        .map(|(name, fingerprints)| LabelNameCardinality {
            name,
            series_count: fingerprints.len(),
        })
        .collect::<Vec<_>>();
    cardinality.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    cardinality
}

/// Counts the distinct series carrying each label pair, ordered by descending
/// series count, then by name and value.
pub(crate) fn label_value_cardinality<'a>(
    series: impl IntoIterator<Item = SeriesRef<'a>>,
) -> Vec<LabelValueCardinality> {
    let mut by_value =
        BTreeMap::<(String, crate::PromqlString), BTreeSet<SeriesFingerprint>>::new();
    for SeriesRef { fp, labels } in series {
        for (name, value) in labels.iter() {
            by_value
                .entry((name.clone(), value.clone()))
                .or_default()
                .insert(fp);
        }
    }
    let mut cardinality = by_value
        .into_iter()
        .map(
            |((label_name, label_value), fingerprints)| LabelValueCardinality {
                label_name,
                label_value: label_value.as_str().to_owned(),
                series_count: fingerprints.len(),
            },
        )
        .collect::<Vec<_>>();
    cardinality.sort_by(|left, right| {
        right
            .series_count
            .cmp(&left.series_count)
            .then_with(|| left.label_name.cmp(&right.label_name))
            .then_with(|| left.label_value.cmp(&right.label_value))
    });
    cardinality
}

/// Orders named TSDB statistics by descending value, then by name.
pub(crate) fn named_stats(values: BTreeMap<String, usize>) -> Vec<NamedTsdbStat> {
    let mut stats = values
        .into_iter()
        .map(|(name, value)| NamedTsdbStat { name, value })
        .collect::<Vec<_>>();
    stats.sort_by(|left, right| {
        right
            .value
            .cmp(&left.value)
            .then_with(|| left.name.cmp(&right.name))
    });
    stats
}

/// Summarises the distinct series of a tenant into [`TsdbStats`] beside `head_stats`.
pub(crate) fn tsdb_stats<'a>(
    series: impl IntoIterator<Item = &'a Labels>,
    head_stats: TsdbHeadStats,
) -> TsdbStats {
    let mut by_metric = BTreeMap::<String, usize>::new();
    let mut label_values_by_name = BTreeMap::<String, BTreeSet<String>>::new();
    let mut memory_by_name = BTreeMap::<String, usize>::new();
    let mut by_label_pair = BTreeMap::<String, usize>::new();
    for labels in series {
        if let Some(metric) = labels.get("__name__") {
            *by_metric.entry(metric.to_string()).or_default() += 1;
        }
        for (name, value) in labels.iter() {
            label_values_by_name
                .entry(name.clone())
                .or_default()
                .insert(value.as_str().to_owned());
            *memory_by_name.entry(name.clone()).or_default() += name.len() + value.len();
            *by_label_pair.entry(format!("{name}={value}")).or_default() += 1;
        }
    }

    TsdbStats {
        head_stats,
        series_count_by_metric_name: named_stats(by_metric),
        label_value_count_by_label_name: named_stats(
            label_values_by_name
                .into_iter()
                .map(|(name, values)| (name, values.len()))
                .collect(),
        ),
        memory_in_bytes_by_label_name: named_stats(memory_by_name),
        series_count_by_label_value_pair: named_stats(by_label_pair),
    }
}
