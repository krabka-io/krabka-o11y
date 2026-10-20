use super::{
    CumulativeProfileCache, CumulativeProfileKey, DistributorState, ProfileRecord, ProfilesError,
    TenantId, WalSample, apply_relabel, cap_session_id, enforce_and_reserve_max_series,
    enforce_ingestion_rate, enforce_limits, extract_symbols, require_service_name,
    rollback_reserved_series, split_sample_types,
};

struct DecodedRecord {
    record: ProfileRecord,
    cumulative: bool,
}

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
                    .map(|pair| krabka_blockstore::LabelPair::new(pair.name, pair.value))
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
            decoded.push(DecodedRecord {
                record: rec,
                cumulative: raw.delta,
            });
        }
    }

    let has_cumulative = decoded.iter().any(|record| record.cumulative);
    // Cumulative baselines are a transaction through the WAL acknowledgement.
    // Limited tenants also serialize reservations: otherwise a failed request
    // could remove a fingerprint that a concurrent successful request reused.
    // Ordinary, unlimited ingestion never enters either coordination path.
    let _tenant_guard = if has_cumulative || limits.max_series != 0 {
        Some(
            state
                .tenant_ingest_lock(tenant.as_str())?
                .lock_owned()
                .await,
        )
    } else {
        None
    };
    let mut next_cumulative_profiles = CumulativeProfileCache::new();
    if has_cumulative {
        let cache = state.cumulative_profiles.lock().await;
        for decoded in decoded.iter().filter(|record| record.cumulative) {
            let key = cumulative_key(&decoded.record, tenant.as_str());
            if let Some(previous) = cache.get(&key) {
                next_cumulative_profiles
                    .entry(key)
                    .or_insert_with(|| previous.clone());
            }
        }
    }
    let reservation_records = decoded
        .iter()
        .map(|decoded| decoded.record.clone())
        .collect::<Vec<_>>();
    let reserved = enforce_and_reserve_max_series(state, tenant, &reservation_records)?;
    let mut pending = Vec::with_capacity(decoded.len());
    for decoded in decoded {
        let mut record = decoded.record;
        if decoded.cumulative {
            apply_cumulative_delta(&mut record, tenant.as_str(), &mut next_cumulative_profiles);
        }
        if !record.samples.is_empty() {
            pending.push(record);
        }
    }
    if pending.is_empty() {
        if has_cumulative {
            state
                .cumulative_profiles
                .lock()
                .await
                .extend(next_cumulative_profiles);
        }
        return Ok(());
    }

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

    if has_cumulative {
        state
            .cumulative_profiles
            .lock()
            .await
            .extend(next_cumulative_profiles);
    }

    Ok(())
}

fn cumulative_key(record: &ProfileRecord, tenant: &str) -> CumulativeProfileKey {
    CumulativeProfileKey {
        tenant: tenant.to_owned(),
        labels: record.labels.clone(),
    }
}

fn apply_cumulative_delta(
    record: &mut ProfileRecord,
    tenant: &str,
    cache: &mut CumulativeProfileCache,
) {
    let key = cumulative_key(record, tenant);
    let mut current = std::collections::HashMap::new();
    for sample in &record.samples {
        current.insert(sample.stacktrace_location_refs.clone(), sample.value);
    }
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
            labels: vec![krabka_blockstore::LabelPair::new(
                "__profile_type__",
                "memory:alloc_space:bytes::",
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
        assert_eq!(first.samples.len(), 0);

        let mut second = record(16);
        apply_cumulative_delta(&mut second, "tenant-a", &mut cache);
        assert_eq!(second.samples[0].value, 6);

        let mut reset = record(3);
        apply_cumulative_delta(&mut reset, "tenant-a", &mut cache);
        assert_eq!(reset.samples[0].value, 3);
    }
}
