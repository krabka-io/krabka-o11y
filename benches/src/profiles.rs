//! Populated call trees, for the pprof merge path.

use krabka_pprof::{Frame, Tree};

use crate::Seeded;

/// How many distinct function names the generated stacks draw from at each
/// depth. A small alphabet is what makes two independently generated trees
/// overlap, and overlap is what `Tree::merge` actually does work on: merging
/// two trees that share no node is a concatenation, and merging two that share
/// every node is the case a real profile merge hits.
const NAMES_PER_LEVEL: usize = 8;

/// A tree built from `stacks` stacks of up to `depth` frames.
///
/// `seed` chooses the stacks, so two trees built with different seeds and the
/// same shape overlap heavily without being equal -- which is the pair a merge
/// benchmark wants.
///
/// # Panics
/// Panics when a generated line number does not fit an `i32`, which the
/// bound on it prevents.
#[must_use]
pub fn profile_tree(stacks: usize, depth: usize, seed: u64) -> Tree {
    let mut pick = Seeded::new(seed);
    let mut tree = Tree::new();
    let mut frames: Vec<Frame> = Vec::with_capacity(depth);

    for _ in 0..stacks {
        frames.clear();
        // `add_stack` reads frames leaf-first, so the root is generated last.
        let height = 1 + pick.next_below(depth);
        for level in (0..height).rev() {
            frames.push(Frame {
                function: format!("fn_{level}_{}", pick.next_below(NAMES_PER_LEVEL)),
                file: format!("src/module_{level}.rs"),
                line: i32::try_from(pick.next_below(500)).expect("a line number fits an i32"),
            });
        }
        let value = i64::try_from(pick.next_below(1_000)).expect("a sample value fits an i64") + 1;
        tree.add_stack(&frames, value);
    }
    tree
}
