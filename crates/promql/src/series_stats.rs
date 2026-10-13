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

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct LabelValueKey {
    name: String,
    value: crate::PromqlString,
}

/// Counts the distinct series carrying each label name, ordered by descending
/// series count, then by name.
pub(crate) fn label_name_cardinality<'a>(
    series: impl IntoIterator<Item = SeriesRef<'a>>,
) -> Vec<LabelNameCardinality> {
    let mut by_name = BTreeMap::<String, BTreeSet<SeriesFingerprint>>::new();
    for SeriesRef { fp, labels } in series {
        for pair in labels.iter() {
            by_name.entry(pair.name.clone()).or_default().insert(fp);
        }
    }
    let mut cardinality = Vec::with_capacity(by_name.len());
    for name in by_name.keys() {
        cardinality.push(LabelNameCardinality {
            name: name.clone(),
            series_count: by_name[name].len(),
        });
    }
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
    let mut by_value = BTreeMap::<LabelValueKey, BTreeSet<SeriesFingerprint>>::new();
    for SeriesRef { fp, labels } in series {
        for pair in labels.iter() {
            by_value
                .entry(LabelValueKey {
                    name: pair.name.clone(),
                    value: pair.value.clone(),
                })
                .or_default()
                .insert(fp);
        }
    }
    let mut cardinality = Vec::with_capacity(by_value.len());
    for key in by_value.keys() {
        cardinality.push(LabelValueCardinality {
            label_name: key.name.clone(),
            label_value: key.value.as_str().to_owned(),
            series_count: by_value[key].len(),
        });
    }
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
    let mut stats = Vec::with_capacity(values.len());
    for name in values.keys() {
        stats.push(NamedTsdbStat {
            name: name.clone(),
            value: values[name],
        });
    }
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
        for pair in labels.iter() {
            label_values_by_name
                .entry(pair.name.clone())
                .or_default()
                .insert(pair.value.as_str().to_owned());
            *memory_by_name.entry(pair.name.clone()).or_default() +=
                pair.name.len() + pair.value.len();
            *by_label_pair
                .entry(format!("{}={}", pair.name, pair.value))
                .or_default() += 1;
        }
    }

    TsdbStats {
        head_stats,
        series_count_by_metric_name: named_stats(by_metric),
        label_value_count_by_label_name: {
            let mut values = BTreeMap::new();
            for name in label_values_by_name.keys() {
                values.insert(name.clone(), label_values_by_name[name].len());
            }
            named_stats(values)
        },
        memory_in_bytes_by_label_name: named_stats(memory_by_name),
        series_count_by_label_value_pair: named_stats(by_label_pair),
    }
}
