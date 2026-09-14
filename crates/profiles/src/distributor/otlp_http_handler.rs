use super::*;

pub(crate) async fn otlp_http_handler(
    Extension(state): Extension<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let start = std::time::Instant::now();
    let bytes = body.len() as u64;
    let mut items: u64 = 0;
    let tenant = tenant_from_headers(&headers, &state.tenant_policy);
    let json = is_json(&headers);
    let response_is_json = matches!(json, Ok(true));
    // ONE server span per ingest request (not per sample). `krabka.ingest.samples`
    // is filled in after the body runs and the item count is known.
    let ingest_span = tracing::info_span!(
        "profiles_ingest",
        otel.kind = "server",
        messaging.system = "kafka",
        messaging.destination.name = PROFILES_WAL_TOPIC,
        krabka.tenant = ingest_span_tenant(tenant.as_ref().ok()),
        krabka.ingest.samples = tracing::field::Empty,
        krabka.ingest.bytes = bytes,
    );
    let result = async {
        let tenant = tenant.as_ref().map_err(TenantResolveError::clone)?;
        // Before the profiles are decoded, so a denied push reaches no WAL.
        authorize_tenant(&principal, tenant)?;
        let body = decode_http_body(&headers, &body, state.max_decompressed)?;
        let json = json?;
        let req = if json {
            decode_otlp_json(&body)?
        } else {
            pb::otlp_profiles::ExportProfilesServiceRequest::decode(body.as_slice())
                .map_err(|err| ProfilesError::Decode(format!("OTLP profiles decode: {err}")))?
        };
        let raws = decode_otlp(&req)?;
        items = raws.len() as u64;
        process_raw(&state, tenant, raws).await?;
        let response = pb::otlp_profiles::ExportProfilesServiceResponse {
            partial_success: None,
        };
        if json {
            serde_json::to_vec(&response)
                .map(|body| ("application/json", body))
                .map_err(|err| ProfilesError::Internal(format!("OTLP profiles JSON encode: {err}")))
        } else {
            Ok(("application/x-protobuf", response.encode_to_vec()))
        }
    }
    .instrument(ingest_span.clone())
    .await;

    ingest_span.record("krabka.ingest.samples", items);
    if let Ok(tenant) = &tenant {
        state.metrics.record_ingest_samples(tenant.as_str(), items);
    }
    state.metrics.record_ingest(
        result.is_ok(),
        IngestBytes(bytes),
        IngestItems(items),
        start.elapsed().as_time(),
    );
    match result {
        Ok((content_type, body)) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, content_type)],
            Bytes::from(body),
        )
            .into_response(),
        Err(err) => otlp_error_response(&err, response_is_json),
    }
}

fn decode_otlp_json(
    body: &[u8],
) -> Result<pb::otlp_profiles::ExportProfilesServiceRequest, ProfilesError> {
    let mut value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|err| ProfilesError::Decode(format!("OTLP profiles JSON decode: {err}")))?;
    rewrite_link_ids(&mut value)?;
    serde_json::from_value(value)
        .map_err(|err| ProfilesError::Decode(format!("OTLP profiles JSON decode: {err}")))
}

fn rewrite_link_ids(value: &mut serde_json::Value) -> Result<(), ProfilesError> {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(serde_json::Value::Array(links)) = object.get_mut("linkTable") {
                for link in links {
                    let Some(link) = link.as_object_mut() else {
                        continue;
                    };
                    for name in ["traceId", "trace_id", "spanId", "span_id"] {
                        let Some(id) = link.get_mut(name) else {
                            continue;
                        };
                        let text = id.as_str().ok_or_else(|| {
                            ProfilesError::Decode(format!("OTLP link {name} must be hexadecimal"))
                        })?;
                        let bytes = decode_hex(text).ok_or_else(|| {
                            ProfilesError::Decode(format!("OTLP link {name} must be hexadecimal"))
                        })?;
                        *id = serde_json::Value::String(
                            base64::engine::general_purpose::STANDARD.encode(bytes),
                        );
                    }
                }
            }
            for child in object.values_mut() {
                rewrite_link_ids(child)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                rewrite_link_ids(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return None;
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16)?;
            let low = (pair[1] as char).to_digit(16)?;
            u8::try_from((high << 4) | low).ok()
        })
        .collect()
}

#[derive(prost::Message, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct OtlpStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

fn otlp_error_response(err: &ProfilesError, json: bool) -> Response {
    let status =
        StatusCode::from_u16(err.status_code()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let code = match status {
        StatusCode::FORBIDDEN => 7,
        StatusCode::TOO_MANY_REQUESTS => 8,
        StatusCode::INTERNAL_SERVER_ERROR => 13,
        _ => 3,
    };
    let envelope = OtlpStatus {
        code,
        message: client_facing_message(err),
    };
    let (content_type, body) = if json {
        (
            "application/json",
            serde_json::to_vec(&envelope).unwrap_or_default(),
        )
    } else {
        ("application/x-protobuf", envelope.encode_to_vec())
    };
    (
        status,
        [(axum::http::header::CONTENT_TYPE, content_type)],
        body,
    )
        .into_response()
}

fn is_json(headers: &HeaderMap) -> Result<bool, ProfilesError> {
    let content_type = match headers.get(axum::http::header::CONTENT_TYPE) {
        Some(value) => value
            .to_str()
            .map_err(|_| ProfilesError::UnsupportedFormat("non-ASCII content-type".into()))?,
        None => "application/x-protobuf",
    }
    .split(';')
    .next()
    .unwrap_or_default()
    .trim();
    if content_type.eq_ignore_ascii_case("application/json") {
        Ok(true)
    } else if content_type.eq_ignore_ascii_case("application/protobuf")
        || content_type.eq_ignore_ascii_case("application/x-protobuf")
    {
        Ok(false)
    } else {
        Err(ProfilesError::UnsupportedFormat(content_type.to_string()))
    }
}

fn decode_http_body(
    headers: &HeaderMap,
    body: &[u8],
    max_output: ByteSize,
) -> Result<Vec<u8>, ProfilesError> {
    let encoding = match headers.get(axum::http::header::CONTENT_ENCODING) {
        Some(value) => value
            .to_str()
            .map_err(|_| ProfilesError::UnsupportedFormat("non-ASCII content-encoding".into()))?,
        None => "identity",
    };
    if encoding.eq_ignore_ascii_case("gzip") {
        gunzip(body, max_output)
    } else if encoding.eq_ignore_ascii_case("identity") {
        Ok(body.to_vec())
    } else {
        Err(ProfilesError::UnsupportedFormat(format!(
            "content-encoding {encoding}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_link_ids_are_hex_and_unknown_fields_are_ignored() {
        let request = decode_otlp_json(
            br#"{
                "resourceProfiles": [],
                "dictionary": {
                    "linkTable": [{
                        "traceId": "00112233445566778899aabbccddeeff",
                        "spanId": "0102030405060708",
                        "futureLinkField": true
                    }],
                    "futureDictionaryField": 1
                },
                "futureRequestField": "ignored"
            }"#,
        )
        .unwrap();
        let link = &request.dictionary.unwrap().link_table[0];
        assert2::check!(
            link.trace_id
                == vec![
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                    0xdd, 0xee, 0xff,
                ]
        );
        assert2::check!(link.span_id == (1_u8..=8).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn errors_use_google_status_in_the_request_wire_format() {
        for (json, content_type) in [
            (true, "application/json"),
            (false, "application/x-protobuf"),
        ] {
            let error = ProfilesError::Decode("bad payload".into());
            let response = otlp_error_response(&error, json);
            assert2::check!(response.status() == StatusCode::BAD_REQUEST);
            assert2::check!(response.headers()[axum::http::header::CONTENT_TYPE] == content_type);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let status = if json {
                let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                OtlpStatus {
                    code: i32::try_from(value["code"].as_i64().unwrap()).unwrap(),
                    message: value["message"].as_str().unwrap().to_string(),
                }
            } else {
                OtlpStatus::decode(body).unwrap()
            };
            assert2::check!(status.code == 3);
            assert2::check!(status.message.contains("bad payload"));
        }
    }
}
