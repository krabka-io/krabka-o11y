use super::*;

/// The `OTLP`/HTTP logs handler must decompress `Content-Encoding: gzip`
/// before it protobuf-decodes. The OpenTelemetry SDK's `otlphttp` exporter,
/// which the demo's Alloy uses, gzips by default, so a regression here
/// means every emitted log line silently fails to decode, and no logs are
/// ingested.
#[test]
pub(crate) fn normalize_otlp_http_logs_decodes_gzip_identically_to_identity() {
    use std::io::Write as _;

    use opentelemetry_proto::tonic::{
        logs::v1::{ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };

    let request = ProtoExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![ProtoKeyValue {
                    key: "service.name".to_string(),
                    value: Some(ProtoAnyValue {
                        value: Some(proto_any_value::Value::StringValue("checkout".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    time_unix_nano: 1_700_000_000_000_000_000,
                    body: Some(ProtoAnyValue {
                        value: Some(proto_any_value::Value::StringValue(
                            "hello world".to_string(),
                        )),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let raw = request.encode_to_vec();

    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "demo".parse().unwrap());
    headers.insert(CONTENT_TYPE, "application/x-protobuf".parse().unwrap());

    // Identity (no Content-Encoding) decodes to a single record.
    let identity = normalize_otlp_http_logs(&headers, &raw, None, None)
        .expect("uncompressed OTLP proto logs should decode");
    assert_eq!(identity.len(), 1);
    assert_eq!(identity[0].line, "hello world");

    // The gzip-compressed body must decode to exactly the same records.
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&raw).unwrap();
    let gzipped = encoder.finish().unwrap();

    let mut gz_headers = headers.clone();
    gz_headers.insert(CONTENT_ENCODING, "gzip".parse().unwrap());
    let from_gzip = normalize_otlp_http_logs(&gz_headers, &gzipped, None, None)
        .expect("gzip-compressed OTLP proto logs should decode");
    assert_eq!(from_gzip, identity);
}
