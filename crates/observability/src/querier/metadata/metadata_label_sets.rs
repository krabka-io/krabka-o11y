use super::*;

pub(crate) async fn metadata_label_sets(
    state: &QuerierState,
    tenant: &str,
    params: &SeriesParams,
) -> Result<Vec<Labels>, HttpQueryError> {
    let time_range = metadata_time_range(params)?;
    let time_fingerprints = if let Some(time_range) = time_range {
        Some(metadata_fingerprints_in_time_range(state, tenant, time_range).await?)
    } else {
        None
    };

    let selectors = metadata_selectors(params)?;
    let mut label_sets = BTreeSet::new();

    for (fingerprint, labels) in state.label_index.tenant_series(tenant) {
        if time_fingerprints
            .as_ref()
            .is_none_or(|fingerprints| fingerprints.contains(&fingerprint))
            && metadata_labels_match_selectors(&labels, &selectors)
        {
            label_sets.insert(metadata_visible_labels(&labels));
        }
    }

    if let Some(hot_tail) = &state.hot_tail {
        // Prune to the metadata window when one is supplied; the per-record bound below
        // (`< start_ns || > end_ns`) is re-applied, so the windowed records are exactly
        // the records a full scan would have kept. With no time range, scan everything.
        let records = match time_range {
            Some(range) => hot_tail
                .source
                .records_in_range(range.start_ns, range.end_ns),
            None => hot_tail.source.records(),
        };
        let frontier = hot_tail.frontier.snapshot();
        for record in records {
            if record.tenant != tenant || frontier.is_compacted(&record) {
                continue;
            }
            if time_range.is_some_and(|range| {
                record.timestamp_ns < range.start_ns || record.timestamp_ns > range.end_ns
            }) {
                continue;
            }
            if metadata_labels_match_selectors(&record.labels, &selectors) {
                label_sets.insert(metadata_visible_labels(&record.labels));
            }
        }
    }

    Ok(label_sets.into_iter().collect())
}
