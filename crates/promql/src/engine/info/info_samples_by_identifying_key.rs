use super::{
    BTreeMap, InstantSample, LabelMatcher, PromqlError, Result, SampleValue,
    compile_label_matchers, info_identifying_key,
};

pub(crate) fn info_samples_by_identifying_key(
    info_samples: Vec<InstantSample>,
    data_label_matchers: &[LabelMatcher],
) -> Result<BTreeMap<String, InstantSample>> {
    // Precompile the regex matchers once before the per-sample loop.
    let compiled = compile_label_matchers(data_label_matchers)?;
    let mut info_by_key = BTreeMap::<String, InstantSample>::new();
    for sample in info_samples {
        if matches!(sample.value, SampleValue::Histogram(_)) {
            return Err(PromqlError::Plan(
                "info series selector must match float samples".to_string(),
            ));
        }
        if !compiled.matches(&sample.labels) {
            continue;
        }
        let Some(key) = info_identifying_key(&sample.labels) else {
            continue;
        };
        info_by_key
            .entry(key)
            .and_modify(|existing| {
                if sample.ts_ms > existing.ts_ms {
                    *existing = sample.clone();
                } else if sample.ts_ms == existing.ts_ms {
                    for (name, value) in sample.labels.iter() {
                        if existing.labels.get(name).is_none() {
                            existing.labels.insert(name, value);
                        }
                    }
                }
            })
            .or_insert(sample);
    }
    Ok(info_by_key)
}
