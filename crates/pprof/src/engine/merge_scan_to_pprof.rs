use super::{
    Arc, BTreeMap, Frame, PprofProfile, ProfileError, ProfileType, ResolvedLocation,
    SampleSelector, ScanMerge, resolved_to_pprof_with_max_nodes, sample_selector_sql,
    selected_sample_rows, stack_matches_call_sites,
};

struct ResolvedSampleLocations {
    locations: Vec<ResolvedLocation>,
    value: i64,
}

pub(crate) async fn merge_scan_to_pprof(
    merge: ScanMerge<'_>,
    profile_type: &ProfileType,
    max_nodes: i64,
) -> Result<PprofProfile, ProfileError> {
    let ScanMerge {
        scan,
        sample_selector,
        call_sites,
    } = merge;
    let sql = sample_selector_sql(scan, sample_selector);
    let batches = scan.collect_sql(&sql).await?;
    let mut samples = BTreeMap::<Vec<ResolvedLocation>, i64>::new();
    for batch in batches {
        let trace_ids = match sample_selector {
            SampleSelector::Trace(trace_ids) => Some(trace_ids),
            SampleSelector::None | SampleSelector::Span(_) => None,
        };
        let rows = selected_sample_rows(&batch, trace_ids)?;
        let symbols = Arc::clone(&scan.symbols);
        let resolved = super::symbolization_worker(move || {
            rows.into_iter()
                .map(|row| ResolvedSampleLocations {
                    locations: symbols.resolve_locations(row.partition, row.stacktrace_id),
                    value: row.value,
                })
                .collect::<Vec<_>>()
        })
        .await?;
        for sample in resolved {
            let frames = sample
                .locations
                .iter()
                .flat_map(|location| &location.lines)
                .map(|line| Frame {
                    function: line.function.name.clone(),
                    file: line.function.filename.clone(),
                    line: i32::try_from(line.line).unwrap_or_default(),
                })
                .collect::<Vec<_>>();
            if !sample.locations.is_empty()
                && (call_sites.is_empty() || stack_matches_call_sites(&frames, call_sites))
            {
                samples
                    .entry(sample.locations)
                    .and_modify(|total| *total = total.saturating_add(sample.value))
                    .or_insert(sample.value);
            }
        }
    }
    Ok(resolved_to_pprof_with_max_nodes(
        samples,
        profile_type,
        max_nodes,
    ))
}
