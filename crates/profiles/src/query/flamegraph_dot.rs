use super::*;

pub(crate) fn flamegraph_dot(flamegraph: &krabka_pprof::FlameGraph) -> String {
    #[derive(Clone)]
    struct DotBar {
        id: usize,
        x_start: i64,
        total: i64,
    }

    let mut dot = String::from("digraph flamegraph {\n  node [shape=box];\n");
    let mut previous = Vec::<DotBar>::new();
    let mut next_id = 0_usize;
    for level in &flamegraph.levels {
        let mut current = Vec::new();
        let mut previous_end = 0_i64;
        for bar in level.values.as_chunks::<4>().0 {
            let x_start = previous_end + bar[0];
            let total = bar[1];
            let self_ = bar[2];
            let name_idx = usize::try_from(bar[3]).unwrap_or(usize::MAX);
            let name = flamegraph
                .names
                .get(name_idx)
                .cloned()
                .unwrap_or_else(|| format!("unknown:{name_idx}"));
            let id = next_id;
            next_id += 1;
            let _ = writeln!(
                dot,
                "  n{id} [label=\"{}\\ntotal={} self={}\"];",
                dot_escape(&name),
                total,
                self_,
            );
            if let Some(parent) = previous
                .iter()
                .find(|parent| x_start >= parent.x_start && x_start < parent.x_start + parent.total)
            {
                let _ = writeln!(dot, "  n{} -> n{id};", parent.id);
            }
            current.push(DotBar { id, x_start, total });
            previous_end = x_start + total;
        }
        previous = current;
    }
    dot.push_str("}\n");
    dot
}
