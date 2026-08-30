use super::*;

/// Decode a `push.v1` `PushRequest` into per-(series, sample) `RawProfile`s.
///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub fn decode_push(
    req: &pb::push::v1::PushRequest,
    max_decompressed: ByteSize,
) -> Result<Vec<RawProfile>, ProfilesError> {
    let mut out = Vec::new();
    for series in &req.series {
        let mut labels = Labels::new();
        for label in &series.labels {
            labels.insert(label.name.clone(), label.value.clone());
        }

        for sample in &series.samples {
            let raw = gunzip(&sample.raw_profile, max_decompressed)?;
            let profile = PprofProfile::decode(&raw)?;
            let mut labels = labels.clone();
            if !sample.id.is_empty() {
                labels.insert("__profile_id__", sample.id.clone());
            }
            out.push(RawProfile {
                labels,
                profile,
                delta: false,
                sample_timestamps_ns: Vec::new(),
                sample_span_ids: Vec::new(),
                sample_trace_ids: Vec::new(),
            });
        }
    }
    Ok(out)
}
