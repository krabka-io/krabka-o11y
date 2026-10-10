use super::{Frame, ProfileError, SampleSelector, Tree, merge_sql_to_tree, sample_selector_sql};

pub(crate) async fn merge_scan_to_tree(
    scan: &crate::ProfileScan,
    tree: &mut Tree,
    prefix_frames: &[Frame],
    sample_selector: SampleSelector<'_>,
    call_sites: &[String],
) -> Result<(), ProfileError> {
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
