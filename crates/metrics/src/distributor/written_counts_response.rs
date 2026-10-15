use super::{HeaderMap, HeaderValue, IntoResponse, Response, StatusCode, WrittenCounts};

pub(crate) fn written_counts_response(status: StatusCode, counts: WrittenCounts) -> Response {
    let mut response = status.into_response();
    let headers = response.headers_mut();
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Samples-Written",
        counts.samples,
    );
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Histograms-Written",
        counts.histograms,
    );
    insert_written_header(
        headers,
        "X-Prometheus-Remote-Write-Exemplars-Written",
        counts.exemplars,
    );
    response
}

pub(crate) fn insert_written_header(headers: &mut HeaderMap, name: &'static str, value: u64) {
    headers.insert(
        name,
        HeaderValue::from_str(&value.to_string()).expect("u64 header value"),
    );
}
