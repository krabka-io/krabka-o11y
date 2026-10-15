//! Raw pprof emission from merged profile trees.

use std::collections::{BTreeMap, HashMap};

use crate::{
    Frame, PprofProfile, ProfileType, ResolvedFunction, ResolvedLocation, ResolvedMapping, Tree,
    proto::{Function, Line, Location, Mapping, Profile, Sample, ValueType},
};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::{Frame, PprofProfile, ProfileType, Tree};

    fn frame(name: &str) -> Frame {
        Frame {
            function: name.to_string(),
            file: String::new(),
            line: 0,
        }
    }

    #[test]
    fn tree_to_pprof_round_trips_and_conserves_total() {
        let profile_type =
            ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        let mut tree = Tree::new();
        tree.add_stack(&[frame("leaf_a"), frame("root_fn")], 7);
        tree.add_stack(&[frame("leaf_b"), frame("root_fn")], 3);

        let profile = tree_to_pprof(&tree, &profile_type);
        let back = PprofProfile::decode(&profile.encode()).unwrap();
        let inner = back.inner();
        let total: i64 = inner
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum();
        let sample_type = inner.sample_type[0];

        check!(total == 10);
        check!(inner.sample.len() == 2);
        check!(inner.sample.iter().all(|sample| sample.value != vec![0]));
        check!(inner.function.iter().all(|function| function.id > 0));
        check!(inner.location.iter().all(|location| location.id > 0));
        check!(
            inner
                .sample
                .iter()
                .flat_map(|sample| sample.location_id.iter())
                .all(|location_id| *location_id > 0)
        );
        check!(
            sample_paths(inner)
                == vec![
                    vec!["leaf_a".to_string(), "root_fn".to_string()],
                    vec!["leaf_b".to_string(), "root_fn".to_string()],
                ]
        );
        check!(inner.string_table[usize::try_from(sample_type.r#type).unwrap()] == "cpu");
        check!(inner.string_table[usize::try_from(sample_type.unit).unwrap()] == "nanoseconds");
    }

    #[test]
    fn tree_to_pprof_with_max_nodes_emits_synthetic_other() {
        let profile_type =
            ProfileType::parse("process_cpu:cpu:nanoseconds:cpu:nanoseconds").unwrap();
        let mut tree = Tree::new();
        for idx in 0..10 {
            tree.add_stack(&[frame(&format!("leaf{idx}"))], idx + 1);
        }

        let profile = tree_to_pprof_with_max_nodes(&tree, &profile_type, 4);
        let inner = profile.inner();
        let total: i64 = inner
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum();

        check!(inner.sample.len() <= 4);
        check!(total == 55);
        check!(inner.string_table.iter().any(|value| value == "other"));
    }

    fn sample_paths(profile: &crate::proto::Profile) -> Vec<Vec<String>> {
        let mut paths = profile
            .sample
            .iter()
            .map(|sample| {
                sample
                    .location_id
                    .iter()
                    .map(|location_id| {
                        let location = profile
                            .location
                            .iter()
                            .find(|location| location.id == *location_id)
                            .expect("location id exists");
                        let function_id = location.line[0].function_id;
                        let function = profile
                            .function
                            .iter()
                            .find(|function| function.id == function_id)
                            .expect("function id exists");
                        profile.string_table[usize::try_from(function.name).unwrap()].clone()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }
}
mod pprof_builder;
mod resolved_to_pprof_with_max_nodes;
use pprof_builder::PprofBuilder;
pub(crate) use resolved_to_pprof_with_max_nodes::resolved_to_pprof_with_max_nodes;

pub(crate) fn collect_samples(
    root: usize,
    node_idx: usize,
    nodes: &[crate::tree::TreeSnapshotNode],
    path: &mut Vec<String>,
    builder: &mut PprofBuilder,
) {
    if node_idx != root {
        path.push(nodes[node_idx].name.clone());
    }
    if nodes[node_idx].self_ != 0 && !path.is_empty() {
        builder.add_sample(path, nodes[node_idx].self_);
    }
    for child in &nodes[node_idx].children {
        collect_samples(root, *child, nodes, path, builder);
    }
    if node_idx != root {
        path.pop();
    }
}

pub(crate) fn intern_string(
    strings: &mut Vec<String>,
    index: &mut HashMap<String, i64>,
    value: &str,
) -> i64 {
    if let Some(slot) = index.get(value) {
        return *slot;
    }
    let slot = i64::try_from(strings.len()).expect("string index fits i64");
    strings.push(value.to_string());
    index.insert(value.to_string(), slot);
    slot
}

#[must_use]
pub fn tree_to_pprof(tree: &Tree, profile_type: &ProfileType) -> PprofProfile {
    let mut builder = PprofBuilder::new(profile_type);
    let snapshot = tree.snapshot();
    let mut path = Vec::new();
    collect_samples(
        snapshot.root,
        snapshot.root,
        &snapshot.nodes,
        &mut path,
        &mut builder,
    );
    builder.finish()
}

#[must_use]
pub fn tree_to_pprof_with_max_nodes(
    tree: &Tree,
    profile_type: &ProfileType,
    max_nodes: i64,
) -> PprofProfile {
    let mut builder = PprofBuilder::new(profile_type);
    for sample in tree.sample_paths(max_nodes) {
        builder.add_sample(&sample.frames, sample.self_value);
    }
    builder.finish()
}
