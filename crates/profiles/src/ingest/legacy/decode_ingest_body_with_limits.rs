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

    let profile = match query.format {
        IngestFormat::Groups => {
            folded_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&body))?
        }
        IngestFormat::Lines => {
            lines_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&body))?
        }
        IngestFormat::Trie => trie_to_pprof(&query.name, &query.units, &body, limits)?,
        IngestFormat::Tree => tree_to_pprof(&query.name, &query.units, &body, limits)?,
        IngestFormat::Speedscope => speedscope_to_pprof(&query.name, &query.units, &body)?,
        IngestFormat::Pprof => {
            return Err(ProfilesError::Invalid(
                "legacy pprof ingest requires multipart `profile` part".to_string(),
            ));
        }
        IngestFormat::Jfr => {
            return Err(ProfilesError::Invalid(
                "legacy jfr ingest requires multipart `jfr` part".to_string(),
            ));
        }
    };
    let profile = apply_query_time(apply_query_sample_rate(profile, query.sample_rate), query)?;
    Ok(RawProfile {
        labels: query_labels(query, Vec::new()),
        profile,
        delta: false,
        sample_timestamps_ns: Vec::new(),
        sample_span_ids: Vec::new(),
        sample_trace_ids: Vec::new(),
    })
}
