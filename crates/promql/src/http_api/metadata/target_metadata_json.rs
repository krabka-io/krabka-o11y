use super::*;

pub(crate) fn target_metadata_json(metadata: Vec<MetadataRecord>) -> Vec<Value> {
    metadata
        .into_iter()
        .map(|record| {
            json!({
                "target": {},
                "metric": record.metric_family_name,
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            })
        })
        .collect()
}
