use super::{
    BTreeMap, DecodedProfile, DecodedSample, HashMap, Labels, ProfileType, ProfilesError,
    RawProfile, labels_key, labels_with_sample_labels,
};

struct SampleTypeGroup {
    labels: Labels,
    profile_type: String,
    samples: Vec<DecodedSample>,
}

/// Split one multi-value pprof into one `DecodedProfile` per `sample_type[]`.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn split_sample_types(raw: &RawProfile) -> Result<Vec<DecodedProfile>, ProfilesError> {
    let default_name = raw
        .labels
        .get("__name__")
        .filter(|name| !name.is_empty())
        .ok_or_else(|| ProfilesError::Invalid("missing __name__".to_string()))?
        .to_string();
    let period_type_and_unit = raw.profile.period_type_strings();
    let period_type = period_type_and_unit.name;
    let period_unit = period_type_and_unit.unit;
    let sample_types = raw.profile.sample_types();
    let timestamp_ns = raw.profile.inner().time_nanos;
    let mut location_refs = HashMap::<u64, u32>::default();
    let mut location_index = 0_usize;
    for location in &raw.profile.inner().location {
        let local_index = u32::try_from(location_index).map_err(|err| {
            ProfilesError::Decode(format!("location index does not fit u32: {err}"))
        })?;
        location_refs.insert(location.id, local_index);
        location_index = location_index.saturating_add(1);
    }
    let mut out = Vec::with_capacity(sample_types.len());
    let mut idx = 0_usize;
    for sample_type_and_unit in sample_types {
        let sample_type = sample_type_and_unit.name;
        let sample_unit = sample_type_and_unit.unit;
        if sample_type.is_empty() || sample_unit.is_empty() {
            return Err(ProfilesError::Decode(format!(
                "sample_type[{idx}] is missing or invalid"
            )));
        }

        let mut groups = BTreeMap::<Vec<krabka_blockstore::LabelPair>, SampleTypeGroup>::new();
        let mut sample_idx = 0_usize;
        for sample in raw.profile.samples() {
            let current_sample_idx = sample_idx;
            sample_idx = sample_idx.saturating_add(1);
            let value = sample
                .value
                .get(idx)
                .copied()
                .ok_or_else(|| ProfilesError::Decode(format!("sample value[{idx}] missing")))?;
            // A pprof with several sample types has samples that belong to
            // only some of them. A Go heap profile's live objects add nothing
            // to `alloc_space`. Pyroscope drops such a sample from the series
            // it adds nothing to, so a stack that is zero here never reaches
            // the flamegraph. If krabka kept it, krabka's flamegraph would
            // hold nodes that Pyroscope's does not, at the same total.
            if value == 0 {
                continue;
            }
            let timestamp_ns = raw
                .sample_timestamps_ns
                .get(current_sample_idx)
                .and_then(|timestamps| timestamps.get(idx))
                .copied()
                .unwrap_or(timestamp_ns);
            let stacktrace_location_refs = sample
                .location_id
                .iter()
                .map(|location| {
                    location_refs.get(location).copied().ok_or_else(|| {
                        ProfilesError::Decode(format!(
                            "sample references missing location id {location}"
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let sample_label = |wanted: &str| {
                sample.label.iter().find_map(|label| {
                    (raw.profile.string(label.key) == Some(wanted))
                        .then(|| raw.profile.string(label.str))
                        .flatten()
                })
            };
            let name = sample_label("__name__").unwrap_or(&default_name);
            let sample_period_type = sample_label("__period_type__").unwrap_or(&period_type);
            let sample_period_unit = sample_label("__period_unit__").unwrap_or(&period_unit);
            let profile_type = ProfileType {
                name: name.to_string(),
                sample_type: sample_type.clone(),
                sample_unit: sample_unit.clone(),
                period_type: sample_period_type.to_string(),
                period_unit: sample_period_unit.to_string(),
                delta: raw.delta,
            }
            .to_string();
            let mut labels = raw.labels.clone();
            labels.insert("__name__", name.to_string());
            labels.insert("__profile_type__", profile_type.clone());
            labels.insert("__period_type__", sample_period_type.to_string());
            labels.insert("__period_unit__", sample_period_unit.to_string());
            labels.insert("__type__", sample_type.clone());
            labels.insert("__unit__", sample_unit.clone());
            if let Some(service_name) = raw.labels.get("service_name") {
                labels.insert("__service_name__", service_name.to_string());
            }
            let sample_labels = labels_with_sample_labels(&labels, &raw.profile, sample);
            let key = labels_key(&sample_labels);
            let group = groups.entry(key).or_insert_with(|| SampleTypeGroup {
                labels: sample_labels,
                profile_type,
                samples: Vec::new(),
            });
            group.samples.push(DecodedSample {
                stacktrace_location_refs,
                value,
                timestamp_ns,
                span_id: raw
                    .sample_span_ids
                    .get(current_sample_idx)
                    .copied()
                    .flatten(),
                trace_id: raw
                    .sample_trace_ids
                    .get(current_sample_idx)
                    .cloned()
                    .flatten(),
            });
        }

        out.extend(groups.into_values().map(|group| DecodedProfile {
            labels: group.labels,
            profile_type: group.profile_type,
            samples: group.samples,
        }));
        idx = idx.saturating_add(1);
    }

    Ok(out)
}
