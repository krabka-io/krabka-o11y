use super::*;

pub(crate) fn decode_loki_http_body(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Vec<u8>, DistributorError> {
    let Some(encoding) = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(body.to_vec());
    };
    let encoding = encoding.trim();

    if encoding.is_empty() || encoding.eq_ignore_ascii_case("snappy") {
        return Ok(body.to_vec());
    } else if encoding.eq_ignore_ascii_case("gzip") {
        let mut decoder = GzDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiGzipDecode)?;
        return Ok(decompressed);
    } else if encoding.eq_ignore_ascii_case("deflate") {
        let mut decoder = DeflateDecoder::new(body);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(DistributorError::LokiDeflateDecode)?;
        return Ok(decompressed);
    }

    Err(DistributorError::UnsupportedLokiContentEncoding(
        encoding.to_string(),
    ))
}
