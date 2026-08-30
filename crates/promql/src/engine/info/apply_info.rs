use super::{InstantSample, BTreeMap, InfoContext, info_identifying_key};

/// Joins input series with overlapping `target_info` series.
pub(crate) fn apply_info(
    samples: Vec<InstantSample>,
    info_by_key: &BTreeMap<String, InstantSample>,
    context: &InfoContext<'_>,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter_map(|mut sample| {
            if sample.labels.get("__name__") == Some("target_info") {
                return Some(sample);
            }
            let key = info_identifying_key(&sample.labels)?;
            let Some(info) = info_by_key.get(&key) else {
                return if context.data_label_selector.is_some()
                    && !context.required_data_label_matchers_match_empty
                {
                    None
                } else {
                    Some(sample)
                };
            };
            for (name, value) in info.labels.iter() {
                if matches!(name.as_str(), "__name__" | "job" | "instance") {
                    continue;
                }
                if context.restrict_data_labels && !context.selected_data_labels.contains(name) {
                    continue;
                }
                if sample.labels.get(name).is_none() {
                    sample.labels.insert(name, value);
                }
            }
            Some(sample)
        })
        .collect()
}
