use super::{PprofBuilder, PprofProfile, ProfileType, Tree};

#[must_use]
pub fn tree_to_pprof_with_max_nodes(
    tree: &Tree,
    profile_type: &ProfileType,
    max_nodes: i64,
) -> PprofProfile {
    let mut builder = PprofBuilder::new(profile_type);
    for (path, value) in tree.sample_paths(max_nodes) {
        builder.add_sample(&path, value);
    }
    builder.finish()
}
