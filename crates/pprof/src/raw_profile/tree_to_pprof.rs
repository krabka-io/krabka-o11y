use super::{PprofBuilder, PprofProfile, ProfileType, Tree, collect_samples};

#[must_use]
pub fn tree_to_pprof(tree: &Tree, profile_type: &ProfileType) -> PprofProfile {
    let mut builder = PprofBuilder::new(profile_type);
    let (root, nodes) = tree.snapshot();
    let mut path = Vec::new();
    collect_samples(root, root, &nodes, &mut path, &mut builder);
    builder.finish()
}
