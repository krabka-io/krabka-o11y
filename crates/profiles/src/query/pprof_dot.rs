use std::fmt::Write;

use num_traits::ToPrimitive;

use super::{BTreeMap, ProfileError, dot_escape, pb};

/// Graphviz function nodes retain symbol associations and caller/callee values.
pub(crate) fn pprof_dot(profile: &pb::google::v1::Profile) -> Result<String, ProfileError> {
    type Key = (String, String, i64, u64);
    let mut nodes: BTreeMap<Key, (i64, i64)> = BTreeMap::new();
    let mut edges: BTreeMap<(Key, Key), i64> = BTreeMap::new();
    let mut total = 0_i64;
    for sample in &profile.sample {
        let value = sample.value.first().copied().unwrap_or_default();
        total = total.saturating_add(value);
        let mut frames = Vec::new();
        for id in &sample.location_id {
            let location = profile
                .location
                .iter()
                .find(|location| location.id == *id)
                .ok_or_else(|| ProfileError::Decode("DOT location missing".into()))?;
            for line in &location.line {
                let function = profile
                    .function
                    .iter()
                    .find(|function| function.id == line.function_id)
                    .ok_or_else(|| ProfileError::Decode("DOT function missing".into()))?;
                let string = |index: i64| {
                    usize::try_from(index)
                        .ok()
                        .and_then(|index| profile.string_table.get(index))
                        .cloned()
                        .unwrap_or_default()
                };
                frames.push((
                    string(function.name),
                    string(function.filename),
                    line.line,
                    location.address,
                ));
            }
        }
        for (index, frame) in frames.iter().enumerate() {
            let node = nodes.entry(frame.clone()).or_default();
            node.0 = node.0.saturating_add(value);
            if index == 0 {
                node.1 = node.1.saturating_add(value);
            }
        }
        for pair in frames.windows(2) {
            let value_entry = edges.entry((pair[1].clone(), pair[0].clone())).or_default();
            *value_entry = value_entry.saturating_add(value);
        }
    }
    let mut dot = String::from("digraph profile {\nnode [shape=box];\n");
    let ids: BTreeMap<_, _> = nodes
        .keys()
        .cloned()
        .enumerate()
        .map(|(id, key)| (key, id + 1))
        .collect();
    for (key, (cumulative, self_value)) in &nodes {
        let id = ids[key];
        let percentage = if total == 0 {
            0.0
        } else {
            self_value.to_f64().unwrap_or_default() * 100.0 / total.to_f64().unwrap_or(1.0)
        };
        let _ = writeln!(
            dot,
            "N{id} [label=\"{}\\n{}:{}\\n{} ({:.2}%)\\nof {}\" tooltip=\"{:016x} {} {}:{} ({})\"];",
            dot_escape(&key.0),
            dot_escape(&key.1),
            key.2,
            self_value,
            percentage,
            cumulative,
            key.3,
            dot_escape(&key.0),
            dot_escape(&key.1),
            key.2,
            cumulative
        );
    }
    for ((caller, callee), value) in edges {
        let weight = if total > 0 {
            (i128::from(value) * 100 + i128::from(total) - 1) / i128::from(total)
        } else {
            0
        };
        let _ = writeln!(
            dot,
            "N{} -> N{} [label=\" {}\" weight={}];",
            ids[&caller], ids[&callee], value, weight
        );
    }
    dot.push_str("}\n");
    Ok(dot)
}
