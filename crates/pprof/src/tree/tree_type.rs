use super::{
    Bar, FlameGraph, Frame, HashMap, HashSet, Level, Node, OTHER_NAME, ROOT_NAME, TreeSnapshotNode,
    append_children, name_slot, sorted_child_position, subtree_self, write_pyroscope_tree_node,
    write_uvarint,
};

/// Symbolized profile tree.
///
/// The root is the synthetic `"total"` node.
#[derive(Clone, Debug)]
pub struct Tree {
    pub(crate) nodes: Vec<Node>,
    pub(crate) root: usize,
}

impl Tree {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: vec![Node {
                name: ROOT_NAME.to_string(),
                total: 0,
                self_: 0,
                children: Vec::new(),
                child_by_name: HashMap::new(),
            }],
            root: 0,
        }
    }

    pub fn add_stack(&mut self, frames: &[Frame], value: i64) {
        // Stackless samples (a goroutine profile occasionally emits one with no
        // frames) carry no position in the flamegraph, so they contribute
        // nothing to the tree — matching Pyroscope, which keeps them in series
        // sums but excludes them from flamegraph/merge-stacktrace totals.
        if frames.is_empty() {
            return;
        }
        let mut current = self.root;
        self.nodes[current].total += value;
        for frame in frames.iter().rev() {
            let name = frame.function.clone();
            let child = if let Some(child) = self.nodes[current].child_by_name.get(&name) {
                *child
            } else {
                let idx = self.nodes.len();
                self.nodes.push(Node {
                    name: name.clone(),
                    total: 0,
                    self_: 0,
                    children: Vec::new(),
                    child_by_name: HashMap::new(),
                });
                let pos = sorted_child_position(&self.nodes[current].children, &self.nodes, &name);
                self.nodes[current].children.insert(pos, idx);
                self.nodes[current].child_by_name.insert(name, idx);
                idx
            };
            current = child;
            self.nodes[current].total += value;
        }
        self.nodes[current].self_ += value;
    }

    pub fn merge(&mut self, other: &Tree) {
        self.merge_node(self.root, other, other.root);
    }

    pub(crate) fn merge_node(&mut self, target: usize, other: &Tree, source: usize) {
        self.nodes[target].total += other.nodes[source].total;
        self.nodes[target].self_ += other.nodes[source].self_;
        for source_child in &other.nodes[source].children {
            let name = &other.nodes[*source_child].name;
            let target_child = if let Some(child) = self.nodes[target].child_by_name.get(name) {
                *child
            } else {
                let idx = self.nodes.len();
                self.nodes.push(Node {
                    name: name.clone(),
                    total: 0,
                    self_: 0,
                    children: Vec::new(),
                    child_by_name: HashMap::new(),
                });
                let pos = sorted_child_position(&self.nodes[target].children, &self.nodes, name);
                self.nodes[target].children.insert(pos, idx);
                self.nodes[target].child_by_name.insert(name.clone(), idx);
                idx
            };
            self.merge_node(target_child, other, *source_child);
        }
    }

    #[must_use]
    pub fn to_flamegraph(self, max_nodes: i64) -> FlameGraph {
        let keep = self.keep_set(max_nodes);
        let mut names = vec![ROOT_NAME.to_string()];
        let mut name_index = HashMap::from([(ROOT_NAME.to_string(), 0_i64)]);
        let mut level_bars: Vec<Vec<[i64; 4]>> = Vec::new();
        let mut stack = vec![Bar {
            node: Some(self.root),
            name: ROOT_NAME.to_string(),
            total: self.nodes[self.root].total,
            self_: self.nodes[self.root].self_,
            x_start: 0,
            level: 0,
        }];
        let mut max_self = 0;

        while let Some(bar) = stack.pop() {
            let name_idx = name_slot(&mut names, &mut name_index, &bar.name);
            if bar.level == level_bars.len() {
                level_bars.push(Vec::new());
            }
            level_bars[bar.level].push([bar.x_start, bar.total, bar.self_, name_idx]);
            max_self = max_self.max(bar.self_);

            let mut children = Vec::new();
            append_children(&self, &keep, &bar, &mut children);
            stack.extend(children);
        }

        let levels = level_bars
            .into_iter()
            .map(|mut bars| {
                bars.reverse();
                let mut values = Vec::with_capacity(bars.len() * 4);
                let mut previous_end = 0;
                for [x_start, total, self_, name_idx] in bars {
                    let delta = x_start - previous_end;
                    values.extend([delta, total, self_, name_idx]);
                    previous_end = x_start + total;
                }
                Level { values }
            })
            .collect();

        FlameGraph {
            total: self.nodes[self.root].total,
            names,
            levels,
            max_self,
        }
    }

    #[must_use]
    pub fn to_pyroscope_tree_bytes(self, max_nodes: i64) -> Vec<u8> {
        if self.nodes[self.root].children.is_empty() {
            return Vec::new();
        }
        let keep = self.keep_set(max_nodes);
        let mut out = Vec::new();
        let root = Bar {
            node: Some(self.root),
            name: String::new(),
            total: self.nodes[self.root].total,
            self_: 0,
            x_start: 0,
            level: 0,
        };
        let mut stack = vec![root];
        while let Some(parent) = stack.pop() {
            write_pyroscope_tree_node(&mut out, &parent.name, parent.self_);
            let mut children = Vec::new();
            append_children(&self, &keep, &parent, &mut children);
            write_uvarint(&mut out, children.len() as u64);
            stack.extend(children);
        }
        out
    }

    pub(crate) fn keep_set(&self, max_nodes: i64) -> HashSet<usize> {
        let max_nodes = usize::try_from(max_nodes.max(1)).unwrap_or(usize::MAX);
        if self.nodes.len() <= max_nodes {
            return (0..self.nodes.len()).collect();
        }
        let parents = self.parents();
        let mut ranked: Vec<usize> = (0..self.nodes.len())
            .filter(|node| *node != self.root)
            .collect();
        ranked.sort_by(|left, right| {
            self.nodes[*right]
                .total
                .cmp(&self.nodes[*left].total)
                .then_with(|| self.nodes[*left].name.cmp(&self.nodes[*right].name))
                .then_with(|| left.cmp(right))
        });
        let mut keep = HashSet::from([self.root]);
        for node in ranked {
            let mut path = Vec::new();
            let mut current = Some(node);
            while let Some(idx) = current {
                if keep.contains(&idx) {
                    break;
                }
                path.push(idx);
                current = parents[idx];
            }
            if keep.len() + path.len() <= max_nodes {
                keep.extend(path);
            }
        }
        keep
    }

    pub(crate) fn parents(&self) -> Vec<Option<usize>> {
        let mut parents = vec![None; self.nodes.len()];
        for (parent, node) in self.nodes.iter().enumerate() {
            for child in &node.children {
                parents[*child] = Some(parent);
            }
        }
        parents
    }

    pub(crate) fn snapshot(&self) -> (usize, Vec<TreeSnapshotNode>) {
        (
            self.root,
            self.nodes
                .iter()
                .map(|node| TreeSnapshotNode {
                    name: node.name.clone(),
                    total: node.total,
                    self_: node.self_,
                    children: node.children.clone(),
                })
                .collect(),
        )
    }

    pub(crate) fn sample_paths(&self, max_nodes: i64) -> Vec<(Vec<String>, i64)> {
        let keep = self.keep_set(max_nodes);
        let mut samples = Vec::new();
        let mut path = Vec::new();
        self.collect_sample_paths(self.root, &keep, &mut path, &mut samples);
        samples
    }

    pub(crate) fn collect_sample_paths(
        &self,
        node_idx: usize,
        keep: &HashSet<usize>,
        path: &mut Vec<String>,
        samples: &mut Vec<(Vec<String>, i64)>,
    ) {
        if node_idx != self.root {
            path.push(self.nodes[node_idx].name.clone());
        }
        if self.nodes[node_idx].self_ != 0 && !path.is_empty() {
            samples.push((path.clone(), self.nodes[node_idx].self_));
        }
        let mut other_self = 0;
        for child in &self.nodes[node_idx].children {
            if keep.contains(child) {
                self.collect_sample_paths(*child, keep, path, samples);
            } else {
                other_self += subtree_self(self, *child);
            }
        }
        if other_self != 0 {
            let mut other_path = path.clone();
            other_path.push(OTHER_NAME.to_string());
            samples.push((other_path, other_self));
        }
        if node_idx != self.root {
            path.pop();
        }
    }

    #[cfg(test)]
    pub(crate) fn total_of(&self, path: &[&str]) -> i64 {
        self.node_at(path).total
    }

    #[cfg(test)]
    pub(crate) fn self_of(&self, path: &[&str]) -> i64 {
        self.node_at(path).self_
    }

    #[cfg(test)]
    pub(crate) fn node_at(&self, path: &[&str]) -> &Node {
        assert!(!path.is_empty());
        assert_eq!(path[0], ROOT_NAME);
        let mut idx = self.root;
        for name in &path[1..] {
            idx = *self.nodes[idx]
                .child_by_name
                .get(*name)
                .expect("path exists");
        }
        &self.nodes[idx]
    }
}

impl Default for Tree {
    fn default() -> Self {
        Self::new()
    }
}
