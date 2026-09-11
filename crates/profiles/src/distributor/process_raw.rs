use super::{
    DistributorState, ProfileRecord, ProfilesError, TenantId, WalSample, apply_relabel,
    cap_session_id, enforce_and_reserve_max_series, enforce_ingestion_rate, enforce_limits,
    extract_symbols, require_service_name, rollback_reserved_series, split_sample_types,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn process_raw(
    state: &DistributorState,
    tenant: &TenantId,
    raws: Vec<crate::ingest::RawProfile>,
) -> Result<(), ProfilesError> {
    // One resolution of this tenant's limits for the whole request. Every gate
    // below reads the same values, and an unlisted tenant gets the overrides
    // file's defaults.
    let limits = state.overrides.for_tenant(tenant);
    let mut pending = Vec::new();
    for mut raw in raws {
        if !apply_relabel(&mut raw.labels, &state.relabel) {
            continue;
        }
        require_service_name(&mut raw.labels);
        cap_session_id(&mut raw.labels, limits);

        let symbols = extract_symbols(&raw.profile)?;
        for profile in split_sample_types(&raw)? {
            enforce_limits(&profile.labels, limits)?;
            let rec = ProfileRecord {
                tenant: tenant.as_str().to_owned(),
                labels: profile
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
                profile_type: profile.profile_type,
                samples: profile
                    .samples
                    .into_iter()
                    .map(|sample| WalSample {
                        stacktrace_location_refs: sample.stacktrace_location_refs,
                        value: sample.value,
                        timestamp_ns: sample.timestamp_ns,
                        span_id: sample.span_id,
                        trace_id: sample.trace_id,
                    })
                    .collect(),
                symbols: symbols.clone(),
            };
            pending.push(rec);
        }
    }

    // Atomically check the max-series limit AND reserve the new fingerprints
    // under a single lock hold (see `enforce_and_reserve_max_series`). The
    // returned set lists fingerprints that were newly inserted by this call and
    // must be rolled back if the subsequent WAL append fails, so a rejected or
    // failed write never permanently inflates the tenant's series count.
    let reserved = enforce_and_reserve_max_series(state, tenant, &pending)?;
    if let Err(err) = enforce_ingestion_rate(state, tenant, pending.len()) {
        rollback_reserved_series(state, tenant.as_str(), &reserved);
        return Err(err);
    }

    // One pipelined batch, not one produce per record. The sink enqueues the
    // records in this order, so one series' records stay ordered on the
    // partition its key selects.
    if let Err(error) = state.sink.append_batch(pending).await {
        // The WAL append failed: count it as a WAL/produce failure (distinct
        // from a 4xx client/validation rejection) and undo the series
        // reservation so a transient produce error doesn't leak into the
        // tenant's max-series budget.
        state.metrics.record_wal_append_failure();
        // Profiles have no query-time deduplication, so a retry of a request
        // that appended in part writes those samples a second time.
        state
            .metrics
            .wal_produce
            .record_batch_failure(error.appended(), error.total());
        rollback_reserved_series(state, tenant.as_str(), &reserved);
        return Err(ProfilesError::from(error));
    }

    Ok(())
}
