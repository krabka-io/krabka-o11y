use super::{BTreeMap, MetadataRecord, Value, json};

pub(crate) fn metadata_json(
    metadata: Vec<MetadataRecord>,
    limit_per_metric: Option<usize>,
) -> BTreeMap<String, Vec<Value>> {
    let mut by_metric = BTreeMap::<String, Vec<Value>>::new();
    for record in metadata {
        let entries = by_metric.entry(record.metric_family_name).or_default();
        if limit_per_metric == Some(0) || limit_per_metric.is_none_or(|limit| entries.len() < limit)
        {
            entries.push(json!({
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            }));
        }
    }
    by_metric
}
