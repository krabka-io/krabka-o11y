use super::*;

/// Decode legacy ingest with explicit expansion limits.
///
/// # Errors
/// Returns an error when the request is invalid or exceeds a configured limit.
pub async fn decode_ingest_body_with_limits(
    query: &IngestQuery,
    content_type: Option<&str>,
    body: bytes::Bytes,
    max: ByteSize,
    limits: LegacyDecodeLimits,
) -> Result<RawProfile, ProfilesError> {
    if let Some(content_type) = content_type
        && content_type
            .split(';')
            .next()
            .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("multipart/form-data"))
    {
        return decode_ingest_multipart_with_limits(query, content_type, body, max, limits).await;
    }

    if body.len() > max.bytes_usize() {
        return Err(ProfilesError::TooLarge {
            limit: max.bytes_usize(),
        });
    }

    // A pprof body carries its own sample types, so it keeps them and takes the
    // metric name that follows from them. The stack formats carry none, and
    // Pyroscope reads every one of them as a CPU profile.
    let (profile, metric_name) = match query.format {
        IngestFormat::Pprof => {
            let profile = PprofProfile::decode(&maybe_gunzip(&body, max)?)?;
            let metric_name = pprof_metric_name(&profile).ok_or_else(|| {
                ProfilesError::Decode("pprof profile declares no sample_type".to_string())
            })?;
            (profile, metric_name)
        }
        IngestFormat::Groups => legacy_cpu_profile(
            folded_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&body))?,
            query,
        ),
        IngestFormat::Lines => legacy_cpu_profile(
            lines_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&body))?,
            query,
        ),
        IngestFormat::Trie => legacy_cpu_profile(
            trie_to_pprof(&query.name, &query.units, &body, limits)?,
            query,
        ),
        IngestFormat::Tree => legacy_cpu_profile(
            tree_to_pprof(&query.name, &query.units, &body, limits)?,
            query,
        ),
        IngestFormat::Speedscope => legacy_cpu_profile(
            speedscope_to_pprof(&query.name, &query.units, &body)?,
            query,
        ),
        IngestFormat::Jfr => {
            return Err(ProfilesError::Invalid(
                "legacy jfr ingest requires multipart `jfr` part".to_string(),
            ));
        }
    };
    let profile = apply_query_time(profile, query)?;
    Ok(RawProfile {
        labels: query_labels(query, &metric_name, Vec::new()),
        profile,
        delta: false,
        sample_timestamps_ns: Vec::new(),
        sample_span_ids: Vec::new(),
        sample_trace_ids: Vec::new(),
    })
}
