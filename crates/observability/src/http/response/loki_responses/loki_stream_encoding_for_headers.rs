use super::{CATEGORIZE_LABELS_ENCODING_FLAG, HeaderMap, LokiStreamEncoding, loki_encoding_flags};

/// The `streams` encoding a request asked for.
///
/// Loki matches the flag exactly: `CATEGORIZE-LABELS` is echoed back but
/// changes nothing.
pub(crate) fn loki_stream_encoding_for_headers(headers: &HeaderMap) -> LokiStreamEncoding {
    if loki_encoding_flags(headers)
        .iter()
        .any(|flag| flag == CATEGORIZE_LABELS_ENCODING_FLAG)
    {
        return LokiStreamEncoding::CategorizeLabels;
    }
    LokiStreamEncoding::Folded
}
