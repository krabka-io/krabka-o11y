use super::{Labels, append_len_prefixed, xxh3_64};

pub type SeriesFingerprint = u64;

#[must_use]
pub fn series_fingerprint(labels: &Labels) -> SeriesFingerprint {
    let mut canonical = Vec::new();
    for (name, value) in labels {
        append_len_prefixed(&mut canonical, name);
        append_len_prefixed(&mut canonical, value);
    }
    xxh3_64(&canonical)
}
