use super::*;

pub(crate) async fn remote_read<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match tenant_from_headers(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = require_remote_read_headers(&headers) {
        return error.into_response();
    }

    let decompressed = match snappy_block_decode(&body, state.remote_read_max_body) {
        Ok(decompressed) => decompressed,
        Err(error) => return ApiError::from(error).into_response(),
    };
    let request = match pb::v1::ReadRequest::decode(decompressed.as_slice()) {
        Ok(request) => request,
        Err(error) => {
            return ApiError::bad_data(format!("protobuf decode failed: {error}")).into_response();
        }
    };
    if let Err(error) = require_remote_read_samples_response(&request) {
        return error.into_response();
    }

    let response = match remote_read_response(state.as_ref(), &tenant, request).await {
        Ok(response) => response,
        Err(error) => return error.into_response(),
    };
    let encoded = response.encode_to_vec();
    let compressed = match snap::raw::Encoder::new().compress_vec(&encoded) {
        Ok(compressed) => compressed,
        Err(error) => {
            return ApiError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                error_type: "execution",
                message: format!("snappy encode failed: {error}"),
            }
            .into_response();
        }
    };

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "application/x-protobuf"),
            (header::CONTENT_ENCODING, "snappy"),
        ],
        compressed,
    )
        .into_response()
}
