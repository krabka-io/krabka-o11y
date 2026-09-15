use super::{
    Arc, Array, AsArray, BTreeMap, BinaryArray, Frame, Int64Type, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_TRACE_ID, PCOL_VALUE, PprofProfile, ProfileError, ProfileType,
    ResolvedLocation, SampleSelector, UInt64Type, resolved_to_pprof_with_max_nodes,
    stack_matches_call_sites,
};

pub(crate) async fn merge_scan_to_pprof(
    scan: &crate::ProfileScan,
    profile_type: &ProfileType,
    max_nodes: i64,
    sample_selector: SampleSelector<'_>,
    call_sites: &[String],
) -> Result<PprofProfile, ProfileError> {
    let span_where = match sample_selector {
        SampleSelector::Span(ids) => format!(
            " WHERE {PCOL_SPAN_ID} IN ({})",
            ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
        ),
        SampleSelector::None | SampleSelector::Trace(_) => String::new(),
    };
    let sql = if matches!(sample_selector, SampleSelector::Trace(_)) {
        format!(
            "SELECT {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}, {PCOL_VALUE}, {PCOL_TRACE_ID} \
             FROM {} ORDER BY {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}",
            scan.samples_table
        )
    } else {
        format!(
            "SELECT {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}, SUM({PCOL_VALUE}) AS v \
             FROM {}{span_where} \
             GROUP BY {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID} \
             ORDER BY {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}",
            scan.samples_table
        )
    };
    let batches = scan
        .ctx
        .sql(&sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    let mut samples = BTreeMap::<Vec<ResolvedLocation>, i64>::new();
    for batch in batches {
        let partitions = batch.column(0).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(1).as_primitive::<UInt64Type>();
        let values = batch.column(2).as_primitive::<Int64Type>();
        let traces = matches!(sample_selector, SampleSelector::Trace(_))
            .then(|| batch.column(3).as_binary::<i32>() as &BinaryArray);
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if let (SampleSelector::Trace(wanted), Some(traces)) = (sample_selector, traces)
                && (traces.is_null(row)
                    || !wanted
                        .iter()
                        .any(|trace| trace.as_slice() == traces.value(row)))
            {
                continue;
            }
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            rows.push((partitions.value(row), stacktrace_id, values.value(row)));
        }
        let symbols = Arc::clone(&scan.symbols);
        let resolved = tokio::task::spawn_blocking(move || {
            rows.into_iter()
                .map(|(partition, stacktrace_id, value)| {
                    (symbols.resolve_locations(partition, stacktrace_id), value)
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|err| ProfileError::Symbolize(format!("symbolization worker failed: {err}")))?;
        for (locations, value) in resolved {
            let frames = locations
                .iter()
                .flat_map(|location| &location.lines)
                .map(|line| Frame {
                    function: line.function.name.clone(),
                    file: line.function.filename.clone(),
                    line: i32::try_from(line.line).unwrap_or_default(),
                })
                .collect::<Vec<_>>();
            if !locations.is_empty()
                && (call_sites.is_empty() || stack_matches_call_sites(&frames, call_sites))
            {
                samples
                    .entry(locations)
                    .and_modify(|total| *total = total.saturating_add(value))
                    .or_insert(value);
            }
        }
    }
    Ok(resolved_to_pprof_with_max_nodes(
        samples,
        profile_type,
        max_nodes,
    ))
}
