//! Raw pprof emission from merged profile trees.

use std::collections::HashMap;

use crate::{
    PprofProfile, ProfileType, Tree,
    proto::{Function, Line, Location, Profile, Sample, ValueType},
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
            tree.add_stack(&[frame(&format!("leaf{idx}"))], 1);
        }

        let profile = tree_to_pprof_with_max_nodes(&tree, &profile_type, 4);
        let inner = profile.inner();
        let total: i64 = inner
            .sample
            .iter()
            .map(|sample| sample.value.iter().sum::<i64>())
            .sum();

        check!(inner.sample.len() <= 4);
        check!(total == 10);
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

// === split-modules: generated submodules ===
mod collect_samples;
mod intern_string;
mod pprof_builder;
mod tree_to_pprof;
mod tree_to_pprof_with_max_nodes;

use collect_samples::collect_samples;
use intern_string::intern_string;
use pprof_builder::PprofBuilder;
pub use tree_to_pprof::tree_to_pprof;
pub use tree_to_pprof_with_max_nodes::tree_to_pprof_with_max_nodes;
