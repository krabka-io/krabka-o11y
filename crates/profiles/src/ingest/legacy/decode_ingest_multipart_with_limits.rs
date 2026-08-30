use super::*;

/// Decode multipart legacy ingest with explicit expansion limits.
///
/// # Errors
/// Returns an error when the request is invalid or exceeds a configured limit.
pub async fn decode_ingest_multipart_with_limits(
    query: &IngestQuery,
    content_type: &str,
    body: bytes::Bytes,
    max: ByteSize,
    limits: LegacyDecodeLimits,
) -> Result<RawProfile, ProfilesError> {
    let boundary =
        multer::parse_boundary(content_type).map_err(|e| ProfilesError::Invalid(e.to_string()))?;
    let stream = futures::stream::once(async move { Ok::<_, std::io::Error>(body) });
    let mut multipart = multer::Multipart::new(stream, boundary);
    let mut pprof_bytes = None;
    let mut folded_bytes = None;
    let mut jfr_bytes = None;
    let mut multipart_labels = Vec::new();
    let mut sample_type_config = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ProfilesError::Invalid(e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        let data = field
            .bytes()
            .await
            .map_err(|e| ProfilesError::Invalid(e.to_string()))?;
        if data.len() > max.bytes_usize() {
            return Err(ProfilesError::TooLarge {
                limit: max.bytes_usize(),
            });
        }
        match name.as_str() {
            "profile" if query.format == IngestFormat::Pprof => pprof_bytes = Some(data.to_vec()),
            "sample_type_config" if query.format == IngestFormat::Pprof => {
                sample_type_config = Some(parse_sample_type_config(&data)?);
            }
            "profile" | "groups" | "folded"
                if matches!(query.format, IngestFormat::Groups | IngestFormat::Lines) =>
            {
                folded_bytes = Some(data.to_vec());
            }
            "profile" | "tree" if query.format == IngestFormat::Tree => {
                folded_bytes = Some(data.to_vec());
            }
            "profile" | "trie" if query.format == IngestFormat::Trie => {
                folded_bytes = Some(data.to_vec());
            }
            "profile" | "speedscope" if query.format == IngestFormat::Speedscope => {
                folded_bytes = Some(data.to_vec());
            }
            "jfr" if query.format == IngestFormat::Jfr => jfr_bytes = Some(data.to_vec()),
            "labels" if query.format == IngestFormat::Jfr => {
                multipart_labels = parse_labels_part(&data)?;
            }
            _ => {}
        }
    }

    let delta = sample_type_config
        .as_ref()
        .and_then(|config| config.cumulative)
        .is_some_and(|cumulative| !cumulative);
    let profile = match query.format {
        IngestFormat::Pprof => {
            let raw = pprof_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart `profile` part".to_string())
            })?;
            let profile = PprofProfile::decode(&raw)?;
            if let Some(config) = &sample_type_config {
                apply_sample_type_config(profile, config)
            } else {
                profile
            }
        }
        IngestFormat::Groups => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart folded `profile` part".to_string())
            })?;
            folded_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&raw))?
        }
        IngestFormat::Lines => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart lines `profile` part".to_string())
            })?;
            lines_to_pprof(&query.name, &query.units, &String::from_utf8_lossy(&raw))?
        }
        IngestFormat::Tree => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart tree `profile` part".to_string())
            })?;
            tree_to_pprof(&query.name, &query.units, &raw, limits)?
        }
        IngestFormat::Trie => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart trie `profile` part".to_string())
            })?;
            trie_to_pprof(&query.name, &query.units, &raw, limits)?
        }
        IngestFormat::Speedscope => {
            let raw = folded_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart speedscope `profile` part".to_string())
            })?;
            speedscope_to_pprof(&query.name, &query.units, &raw)?
        }
        IngestFormat::Jfr => {
            let raw = jfr_bytes.ok_or_else(|| {
                ProfilesError::Invalid("missing multipart `jfr` part".to_string())
            })?;
            jfr_to_pprof(&query.name, &raw)?
        }
    };
    let profile = if query.format == IngestFormat::Pprof {
        profile
    } else {
        apply_query_sample_rate(profile, query.sample_rate)
    };
    let profile = apply_query_time(profile, query)?;

    Ok(RawProfile {
        labels: query_labels(query, multipart_labels),
        profile,
        delta,
        sample_timestamps_ns: Vec::new(),
        sample_span_ids: Vec::new(),
        sample_trace_ids: Vec::new(),
    })
}
