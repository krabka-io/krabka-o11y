use super::{ProfilesError, pb};

pub(crate) type OtlpSampleLinks = (Vec<Option<u64>>, Vec<Option<Vec<u8>>>);

pub(crate) fn otlp_sample_links(
    profile: &pb::otlp_profiles::Profile,
    dict: &pb::otlp_profiles::ProfilesDictionary,
) -> Result<OtlpSampleLinks, ProfilesError> {
    let mut span_ids = Vec::with_capacity(profile.samples.len());
    let mut trace_ids = Vec::with_capacity(profile.samples.len());
    for sample in &profile.samples {
        if dict.link_table.is_empty() {
            span_ids.push(None);
            trace_ids.push(None);
            continue;
        }
        let link = usize::try_from(sample.link_index)
            .ok()
            .and_then(|idx| dict.link_table.get(idx))
            .ok_or_else(|| ProfilesError::Invalid("OTLP sample references missing link".into()))?;
        let span_id = if link.span_id.is_empty() {
            None
        } else {
            let bytes: [u8; 8] = link.span_id.as_slice().try_into().map_err(|_| {
                ProfilesError::Invalid("OTLP link span_id must be 8 bytes".to_string())
            })?;
            Some(u64::from_be_bytes(bytes))
        };
        let trace_id = (!link.trace_id.is_empty()).then(|| link.trace_id.clone());
        span_ids.push(span_id);
        trace_ids.push(trace_id);
    }
    Ok((span_ids, trace_ids))
}
