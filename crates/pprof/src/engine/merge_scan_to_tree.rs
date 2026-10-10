use super::{
    Frame, ProfileError, SampleSelector, ScanMerge, Tree, merge_sql_to_tree, sample_selector_sql,
};

pub(crate) async fn merge_scan_to_tree(
    merge: ScanMerge<'_>,
    tree: &mut Tree,
    prefix_frames: &[Frame],
) -> Result<(), ProfileError> {
    let ScanMerge {
        scan,
        sample_selector,
        call_sites,
    } = merge;
    let sql = sample_selector_sql(scan, sample_selector);
    merge_sql_to_tree(
        scan,
        &sql,
        tree,
        prefix_frames,
        call_sites,
        match sample_selector {
            SampleSelector::Trace(ids) => Some(ids),
            SampleSelector::None | SampleSelector::Span(_) => None,
        },
    )
    .await
}
