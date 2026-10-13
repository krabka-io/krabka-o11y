use super::{
    BTreeMap, InfoContext, InstantSample, PromqlError, Result, info_identifying_key, labels_match,
};

/// Joins input series with the selected info metrics at the same identifying key.
pub(crate) fn apply_info(
    samples: Vec<InstantSample>,
    info_by_key: &BTreeMap<String, InstantSample>,
    context: &InfoContext<'_>,
) -> Result<Vec<InstantSample>> {
    let names = context.info_name_matchers();
    let mut output = Vec::new();
    for mut sample in samples {
        if labels_match(&sample.labels, &names)? {
            output.push(sample);
            continue;
        }
        let mut matched = false;
        let mut enrichment = super::Labels::new();
        if let Some(key) = info_identifying_key(&sample.labels) {
            for entry in info_by_key.range(key.clone()..) {
                let candidate = entry.0;
                if !candidate.starts_with(&key) {
                    break;
                }
                let info = entry.1;
                matched = true;
                for pair in info.labels.iter() {
                    let name = pair.name;
                    let value = pair.value;
                    if name == "__name__"
                        || sample.labels.get(name).is_some()
                        || (context.restrict_data_labels
                            && !context.selected_data_labels.contains(name))
                    {
                        continue;
                    }
                    if enrichment
                        .get_value(name)
                        .is_some_and(|previous| previous != value)
                    {
                        return Err(PromqlError::Exec(format!("conflicting label: {name}")));
                    }
                    enrichment.insert(name, value);
                }
            }
        }
        if !matched && !context.required_data_label_matchers_match_empty {
            continue;
        }
        for pair in enrichment.iter() {
            sample.labels.insert(pair.name, pair.value);
        }
        output.push(sample);
    }
    Ok(output)
}
