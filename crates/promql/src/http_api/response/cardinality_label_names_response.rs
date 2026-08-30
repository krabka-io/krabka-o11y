use super::*;

/// Builds the Grafana Mimir `/cardinality/label_names` response from a series set.
///
/// Shape: `{ "label_values_count_total": N, "label_names_count": M,
/// "cardinality": [{ "label_name": .., "label_values_count": k }, ..] }`.
///
/// This function sorts the `cardinality` array by `label_values_count` DESC,
/// then by `label_name` ASC. A `limit` greater than 0 truncates that array. This
/// function computes the two totals over the full, unlimited series set.
pub(crate) fn cardinality_label_names_response(series: &[Labels], limit: Option<usize>) -> Value {
    let mut values_by_name = BTreeMap::<String, BTreeSet<String>>::new();
    for labels in series {
        for (name, value) in labels.iter() {
            values_by_name
                .entry(name.clone())
                .or_default()
                .insert(value.clone());
        }
    }

    let label_names_count = values_by_name.len();
    let label_values_count_total: usize = values_by_name.values().map(BTreeSet::len).sum();

    let mut cardinality = values_by_name
        .into_iter()
        .map(|(name, values)| (name, values.len()))
        .collect::<Vec<_>>();
    cardinality.sort_by(|(left_name, left_count), (right_name, right_count)| {
        right_count
            .cmp(left_count)
            .then_with(|| left_name.cmp(right_name))
    });
    apply_limit(&mut cardinality, limit);

    let entries = cardinality
        .into_iter()
        .map(|(label_name, label_values_count)| {
            json!({
                "label_name": label_name,
                "label_values_count": label_values_count,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "label_values_count_total": label_values_count_total,
        "label_names_count": label_names_count,
        "cardinality": entries,
    })
}
