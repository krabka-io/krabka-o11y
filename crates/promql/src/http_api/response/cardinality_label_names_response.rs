use super::{BTreeMap, BTreeSet, Labels, Value, apply_limit, json};

struct LabelNameCardinalityEntry {
    name: String,
    values_count: usize,
}

/// Builds the Grafana Mimir `/cardinality/label_names` response from a series set.
///
/// Shape: `{ "label_values_count_total": N, "label_names_count": M,
/// "cardinality": [{ "label_name": .., "label_values_count": k }, ..] }`.
///
/// This function sorts the `cardinality` array by `label_values_count` DESC,
/// then by `label_name` ASC. A `limit` greater than 0 truncates that array. This
/// function computes the two totals over the full, unlimited series set.
pub(crate) fn cardinality_label_names_response(series: &[Labels], limit: Option<usize>) -> Value {
    let mut values_by_name = BTreeMap::<String, BTreeSet<crate::PromqlString>>::new();
    for labels in series {
        for pair in labels.iter() {
            values_by_name
                .entry(pair.name.clone())
                .or_default()
                .insert(pair.value.clone());
        }
    }

    let label_names_count = values_by_name.len();
    let label_values_count_total: usize = values_by_name.values().map(BTreeSet::len).sum();

    let mut cardinality = Vec::with_capacity(values_by_name.len());
    for name in values_by_name.keys() {
        cardinality.push(LabelNameCardinalityEntry {
            name: name.clone(),
            values_count: values_by_name[name].len(),
        });
    }
    cardinality.sort_by(|left, right| {
        right
            .values_count
            .cmp(&left.values_count)
            .then_with(|| left.name.cmp(&right.name))
    });
    apply_limit(&mut cardinality, limit);

    let entries = cardinality
        .into_iter()
        .map(|entry| {
            json!({
                "label_name": entry.name,
                "label_values_count": entry.values_count,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "label_values_count_total": label_values_count_total,
        "label_names_count": label_names_count,
        "cardinality": entries,
    })
}
