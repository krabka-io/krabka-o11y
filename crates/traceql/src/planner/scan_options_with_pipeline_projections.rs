use super::*;

pub(crate) fn scan_options_with_pipeline_projections(
    options: &ScanOptions,
    pipeline: &[Pipeline],
) -> ScanOptions {
    let mut options = options.clone();
    for matcher in pipeline_nested_projection_matchers(pipeline) {
        if !options.projection_matchers.contains(&matcher) {
            options.projection_matchers.push(matcher);
        }
    }
    options
}
