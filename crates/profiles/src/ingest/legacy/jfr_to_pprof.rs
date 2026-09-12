use super::{
    IngestQuery, JfrLabels, PprofProfile, ProfilesError, binary_jfr_to_pprof, folded_to_pprof,
};

pub(crate) fn jfr_to_pprof(
    name: &str,
    raw: &[u8],
    query: &IngestQuery,
    labels: &JfrLabels,
) -> Result<PprofProfile, ProfilesError> {
    if raw.starts_with(b"FLR\0") {
        return binary_jfr_to_pprof(name, raw, query, labels);
    }
    let body = std::str::from_utf8(raw).map_err(|err| {
        ProfilesError::Decode(format!("jfr payload is not UTF-8 collapsed stacks: {err}"))
    })?;
    folded_to_pprof(name, "count", body)
}
