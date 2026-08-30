use super::{
    Bar, FlameGraphDiff, HashMap, Level, ROOT_NAME, Tree, append_children, keep_set, merge_node,
    name_slot,
};

#[must_use]
pub fn diff_trees(left: &Tree, right: &Tree, max_nodes: i64) -> FlameGraphDiff {
    let (left_root, left_nodes) = left.snapshot();
    let (right_root, right_nodes) = right.snapshot();
    let mut merged = Vec::new();
    let root = merge_node(
        Some(left_root),
        &left_nodes,
        Some(right_root),
        &right_nodes,
        ROOT_NAME,
        &mut merged,
    );
    let keep = keep_set(&merged, root, max_nodes);
    let mut names = vec![ROOT_NAME.to_string()];
    let mut name_index = HashMap::from([(ROOT_NAME.to_string(), 0_i64)]);
    let mut levels = Vec::new();
    let mut current = vec![Bar {
        node: Some(root),
        name: ROOT_NAME.to_string(),
        total_left: merged[root].total_left,
        self_left: merged[root].self_left,
        total_right: merged[root].total_right,
        self_right: merged[root].self_right,
        x_left: 0,
        x_right: 0,
    }];

    while !current.is_empty() {
        let mut values = Vec::with_capacity(current.len() * 7);
        let mut next = Vec::new();
        let mut previous_left_end = 0;
        let mut previous_right_end = 0;
        for bar in &current {
            let name_idx = name_slot(&mut names, &mut name_index, &bar.name);
            values.extend([
                bar.x_left - previous_left_end,
                bar.total_left,
                bar.self_left,
                bar.x_right - previous_right_end,
                bar.total_right,
                bar.self_right,
                name_idx,
            ]);
            previous_left_end = bar.x_left + bar.total_left;
            previous_right_end = bar.x_right + bar.total_right;
            append_children(&merged, &keep, bar, &mut next);
        }
        levels.push(Level { values });
        current = next;
    }

    FlameGraphDiff {
        names,
        levels,
        left_ticks: merged[root].total_left,
        right_ticks: merged[root].total_right,
    }
}
