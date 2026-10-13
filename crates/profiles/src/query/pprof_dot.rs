use std::fmt::Write;

use num_traits::ToPrimitive;

use super::{BTreeMap, ProfileError, dot_escape, pb};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DotFrame {
    name: String,
    filename: String,
    line: i64,
    address: u64,
}

#[derive(Default)]
struct DotNodeValues {
    cumulative: i64,
    self_value: i64,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DotEdge {
    caller: DotFrame,
    callee: DotFrame,
}

/// Graphviz function nodes retain symbol associations and caller/callee values.
pub(crate) fn pprof_dot(profile: &pb::google::v1::Profile) -> Result<String, ProfileError> {
    let mut nodes: BTreeMap<DotFrame, DotNodeValues> = BTreeMap::new();
    let mut edges: BTreeMap<DotEdge, i64> = BTreeMap::new();
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
                frames.push(DotFrame {
                    name: string(function.name),
                    filename: string(function.filename),
                    line: line.line,
                    address: location.address,
                });
            }
        }
        let mut first_frame = true;
        for frame in &frames {
            let node = nodes.entry(frame.clone()).or_default();
            node.cumulative = node.cumulative.saturating_add(value);
            if first_frame {
                node.self_value = node.self_value.saturating_add(value);
            }
            first_frame = false;
        }
        for pair in frames.windows(2) {
            let value_entry = edges
                .entry(DotEdge {
                    caller: pair[1].clone(),
                    callee: pair[0].clone(),
                })
                .or_default();
            *value_entry = value_entry.saturating_add(value);
        }
    }
    let mut dot = String::from("digraph profile {\nnode [shape=box];\n");
    let mut ids = BTreeMap::new();
    let mut next_id = 1;
    for key in nodes.keys() {
        ids.insert(key.clone(), next_id);
        next_id += 1;
    }
    for key in nodes.keys() {
        let values = &nodes[key];
        let id = ids[key];
        let percentage = if total == 0 {
            0.0
        } else {
            values.self_value.to_f64().unwrap_or_default() * 100.0 / total.to_f64().unwrap_or(1.0)
        };
        let _ = writeln!(
            dot,
            "N{id} [label=\"{}\\n{}:{}\\n{} ({:.2}%)\\nof {}\" tooltip=\"{:016x} {} {}:{} ({})\"];",
            dot_escape(&key.name),
            dot_escape(&key.filename),
            key.line,
            values.self_value,
            percentage,
            values.cumulative,
            key.address,
            dot_escape(&key.name),
            dot_escape(&key.filename),
            key.line,
            values.cumulative
        );
    }
    for edge in edges.keys() {
        let value = edges[edge];
        let weight = if total > 0 {
            (i128::from(value) * 100 + i128::from(total) - 1) / i128::from(total)
        } else {
            0
        };
        let _ = writeln!(
            dot,
            "N{} -> N{} [label=\" {}\" weight={}];",
            ids[&edge.caller], ids[&edge.callee], value, weight
        );
    }
    dot.push_str("}\n");
    Ok(dot)
}
