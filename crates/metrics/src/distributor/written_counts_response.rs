use super::{StatusCode, WrittenCounts, Response, IntoResponse, insert_written_header};

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
