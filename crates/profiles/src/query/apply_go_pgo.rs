use super::{BTreeMap, pb};

/// The pinned Go-PGO resolver trims leaf-first locations, clears callee line
/// numbers, and merges stacks made identical by the transformation.
pub(crate) fn apply_go_pgo(
    profile: &mut pb::google::v1::Profile,
    selector: Option<&pb::types::v1::StackTraceSelector>,
) {
    let Some(pgo) = selector.and_then(|selector| selector.go_pgo.as_ref()) else {
        return;
    };
    let keep = usize::try_from(pgo.keep_locations).unwrap_or(usize::MAX);
    for sample in &mut profile.sample {
        if keep > 0 {
            sample.location_id.truncate(keep);
        }
        if pgo.aggregate_callees
            && let Some(id) = sample.location_id.first()
            && let Some(location) = profile
                .location
                .iter_mut()
                .find(|location| location.id == *id)
            && let Some(line) = location.line.first_mut()
        {
            line.line = 0;
        }
    }
    // Clearing line numbers can make distinct locations equivalent. Resolve
    // those associations before merging sample stacks, as pprof.Merge does.
    let mut locations = BTreeMap::new();
    let mut aliases = BTreeMap::new();
    for location in &profile.location {
        let key = (
            location.mapping_id,
            location.address,
            location.is_folded,
            location
                .line
                .iter()
                .map(|line| (line.function_id, line.line))
                .collect::<Vec<_>>(),
        );
        let id = *locations.entry(key).or_insert(location.id);
        aliases.insert(location.id, id);
    }
    for sample in &mut profile.sample {
        for id in &mut sample.location_id {
            if let Some(canonical) = aliases.get(id) {
                *id = *canonical;
            }
        }
    }
    let mut merged: BTreeMap<Vec<u64>, pb::google::v1::Sample> = BTreeMap::new();
    for sample in std::mem::take(&mut profile.sample) {
        merged
            .entry(sample.location_id.clone())
            .and_modify(|existing| {
                for (value, extra) in existing.value.iter_mut().zip(&sample.value) {
                    *value = value.saturating_add(*extra);
                }
            })
            .or_insert(sample);
    }
    profile.sample = merged.into_values().collect();
    profile.location.retain(|location| {
        profile
            .sample
            .iter()
            .any(|sample| sample.location_id.contains(&location.id))
    });
    profile.function.retain(|function| {
        profile.location.iter().any(|location| {
            location
                .line
                .iter()
                .any(|line| line.function_id == function.id)
        })
    });
    profile.mapping.retain(|mapping| {
        profile
            .location
            .iter()
            .any(|location| location.mapping_id == mapping.id)
    });
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    #[test]
    fn aggregate_callees_merges_distinct_lines_of_the_same_symbol() {
        let mut profile = pb::google::v1::Profile {
            sample: vec![
                pb::google::v1::Sample {
                    location_id: vec![1],
                    value: vec![3],
                    ..Default::default()
                },
                pb::google::v1::Sample {
                    location_id: vec![2],
                    value: vec![7],
                    ..Default::default()
                },
            ],
            location: vec![
                pb::google::v1::Location {
                    id: 1,
                    line: vec![pb::google::v1::Line {
                        function_id: 7,
                        line: 10,
                    }],
                    ..Default::default()
                },
                pb::google::v1::Location {
                    id: 2,
                    line: vec![pb::google::v1::Line {
                        function_id: 7,
                        line: 20,
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut separate = profile.clone();
        let mut selector = pb::types::v1::StackTraceSelector {
            go_pgo: Some(pb::types::v1::GoPgo {
                keep_locations: 1,
                aggregate_callees: false,
            }),
            ..Default::default()
        };
        apply_go_pgo(&mut separate, Some(&selector));
        check!(separate.sample.len() == 2);
        selector.go_pgo.as_mut().unwrap().aggregate_callees = true;
        apply_go_pgo(&mut profile, Some(&selector));
        check!(profile.sample.len() == 1);
        check!(profile.sample[0].value == vec![10]);
        check!(profile.location.len() == 1 && profile.location[0].line[0].line == 0);
    }

    #[test]
    fn pgo_trims_and_merges_leaf_stacks_without_losing_values() {
        let mut profile = pb::google::v1::Profile {
            sample: vec![
                pb::google::v1::Sample {
                    location_id: vec![1, 2],
                    value: vec![3],
                    ..Default::default()
                },
                pb::google::v1::Sample {
                    location_id: vec![1, 3],
                    value: vec![7],
                    ..Default::default()
                },
            ],
            location: (1..=3)
                .map(|id| pb::google::v1::Location {
                    id,
                    line: vec![pb::google::v1::Line {
                        function_id: id,
                        line: 10,
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let selector = pb::types::v1::StackTraceSelector {
            go_pgo: Some(pb::types::v1::GoPgo {
                keep_locations: 1,
                aggregate_callees: true,
            }),
            ..Default::default()
        };
        apply_go_pgo(&mut profile, Some(&selector));
        check!(profile.sample.len() == 1);
        check!(profile.sample[0].value == vec![10]);
        check!(profile.sample[0].location_id == vec![1]);
        check!(profile.location.len() == 1);
        check!(profile.location[0].line[0].line == 0);
    }
}
