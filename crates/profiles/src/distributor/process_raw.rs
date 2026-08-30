use super::{
    DistributorState, ProfileRecord, ProfilesError, WalSample, apply_relabel, cap_session_id,
    enforce_and_reserve_max_series, enforce_ingestion_rate, enforce_limits, extract_symbols,
    ingest_limits_for_tenant, require_service_name, rollback_reserved_series, split_sample_types,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn process_raw(
    state: &DistributorState,
    tenant: &str,
    raws: Vec<crate::ingest::RawProfile>,
) -> Result<(), ProfilesError> {
    let mut pending = Vec::new();
    for mut raw in raws {
        if !apply_relabel(&mut raw.labels, &state.relabel) {
            continue;
        }
        require_service_name(&mut raw.labels);
        let limits = ingest_limits_for_tenant(state, tenant);
        cap_session_id(&mut raw.labels, limits.session_id_buckets);

        let symbols = extract_symbols(&raw.profile)?;
        for profile in split_sample_types(&raw)? {
            enforce_limits(&profile.labels, &limits)?;
            let rec = ProfileRecord {
                tenant: tenant.to_string(),
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
        rollback_reserved_series(state, tenant, &reserved);
        return Err(err);
    }

    for rec in pending {
        if let Err(err) = state.sink.append(rec).await {
            // The WAL append failed: count it as a WAL/produce failure (distinct
            // from a 4xx client/validation rejection) and undo the series
            // reservation so a transient produce error doesn't leak into the
            // tenant's max-series budget.
            state.metrics.record_wal_append_failure();
            rollback_reserved_series(state, tenant, &reserved);
            return Err(err);
        }
    }

    Ok(())
}
