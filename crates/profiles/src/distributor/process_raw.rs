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
    let mut decoded = Vec::new();
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
            decoded.push((rec, raw.delta));
        }
    }

    let mut cumulative_profiles = state.cumulative_profiles.lock().await;
    let mut next_cumulative_profiles = cumulative_profiles.clone();
    let mut pending = Vec::with_capacity(decoded.len());
    for (mut record, cumulative) in decoded {
        if cumulative {
            apply_cumulative_delta(&mut record, tenant.as_str(), &mut next_cumulative_profiles);
        }
        if !record.samples.is_empty() {
            pending.push(record);
        }
    }
    if pending.is_empty() {
        *cumulative_profiles = next_cumulative_profiles;
        return Ok(());
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

    *cumulative_profiles = next_cumulative_profiles;

    Ok(())
}

fn apply_cumulative_delta(
    record: &mut ProfileRecord,
    tenant: &str,
    cache: &mut std::collections::HashMap<
        (String, Vec<(String, String)>),
        std::collections::HashMap<Vec<u32>, i64>,
    >,
) {
    let key = (tenant.to_string(), record.labels.clone());
    let current = record
        .samples
        .iter()
        .map(|sample| (sample.stacktrace_location_refs.clone(), sample.value))
        .collect::<std::collections::HashMap<_, _>>();
    let previous = cache.insert(key, current);
    let Some(previous) = previous else {
        record.samples.clear();
        return;
    };
    for sample in &mut record.samples {
        let prior = previous
            .get(&sample.stacktrace_location_refs)
            .copied()
            .unwrap_or(0);
        sample.value = if sample.value >= prior {
            sample.value - prior
        } else {
            sample.value
        };
    }
    record.samples.retain(|sample| sample.value != 0);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(value: i64) -> ProfileRecord {
        ProfileRecord {
            tenant: "tenant-a".into(),
            labels: vec![(
                "__profile_type__".into(),
                "memory:alloc_space:bytes::".into(),
            )],
            profile_type: "memory:alloc_space:bytes::".into(),
            samples: vec![WalSample {
                stacktrace_location_refs: vec![1, 2],
                value,
                timestamp_ns: 1,
                span_id: None,
                trace_id: None,
            }],
            symbols: crate::WalSymbolSet {
                strings: Vec::new(),
                functions: Vec::new(),
                locations: Vec::new(),
                mappings: Vec::new(),
            },
        }
    }

    #[test]
    fn cumulative_profiles_are_seeded_then_subtracted_and_reset() {
        let mut cache = std::collections::HashMap::new();
        let mut first = record(10);
        apply_cumulative_delta(&mut first, "tenant-a", &mut cache);
        assert!(first.samples.is_empty());

        let mut second = record(16);
        apply_cumulative_delta(&mut second, "tenant-a", &mut cache);
        assert_eq!(second.samples[0].value, 6);

        let mut reset = record(3);
        apply_cumulative_delta(&mut reset, "tenant-a", &mut cache);
        assert_eq!(reset.samples[0].value, 3);
    }
}
