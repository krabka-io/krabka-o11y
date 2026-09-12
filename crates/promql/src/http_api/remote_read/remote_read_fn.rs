use super::{
    ApiError, Arc, Body, Bytes, Extension, HeaderMap, IntoResponse, Message, MetricStore,
    Principal, PrometheusApiState, Response, State, StatusCode, authorized_tenant_from_headers,
    encode_chunked_read_frames, header, negotiate_remote_read_response_type, pb,
    remote_read_response, require_remote_read_headers, snappy_block_decode,
};

pub(crate) async fn remote_read<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
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
    let mut response_type = match negotiate_remote_read_response_type(&request) {
        Ok(response_type) => response_type,
        Err(error) => return error.into_response(),
    };
    let samples_fallback = request
        .accepted_response_types
        .contains(&(pb::v1::ResponseType::Samples as i32));

    let response = match remote_read_response(state.as_ref(), &tenant, request).await {
        Ok(response) => response,
        Err(error) => return error.into_response(),
    };
    // Prometheus v3.8 also permits HISTOGRAM and FLOAT_HISTOGRAM
    // encoded chunks on this response type. Until those codecs exist here,
    // preserve histogram values through SAMPLES or reject the request.
    if response_type == pb::v1::ResponseType::StreamedXorChunks
        && response.results.iter().any(|result| {
            result
                .timeseries
                .iter()
                .any(|series| !series.histograms.is_empty())
        })
    {
        if samples_fallback {
            response_type = pb::v1::ResponseType::Samples;
        } else {
            return ApiError {
                status: StatusCode::UNPROCESSABLE_ENTITY,
                error_type: "execution",
                message: "STREAMED_XOR_CHUNKS native-histogram encoding is not implemented; request a SAMPLES fallback".into(),
            }
            .into_response();
        }
    }

    if response_type == pb::v1::ResponseType::StreamedXorChunks {
        let stream = futures::stream::iter(
            encode_chunked_read_frames(response).map(|frame| frame.map(Bytes::from)),
        );
        return (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse",
            )],
            Body::from_stream(stream),
        )
            .into_response();
    }

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
