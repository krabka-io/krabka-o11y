use super::*;

#[test]
pub(crate) fn timestamp_and_value_conversions_cover_json_and_proto_shapes() {
    assert_eq!(otlp_timestamp_ns(&json!("123")).unwrap(), 123);
    assert_eq!(otlp_timestamp_ns(&json!(456)).unwrap(), 456);
    assert!(otlp_timestamp_ns(&json!(-1)).is_err());
    assert_eq!(
        otlp_severity_number_to_string(&json!("INFO")).unwrap(),
        "INFO"
    );
    assert_eq!(otlp_severity_number_to_string(&json!(9)).unwrap(), "9");

    let otlp_value = OtlpAnyValue::Kvlist(OtlpKeyValueList {
        values: Some(vec![
            OtlpKeyValue {
                key: "ok".to_string(),
                value: OtlpAnyValue::Bool(true),
            },
            OtlpKeyValue {
                key: "items".to_string(),
                value: OtlpAnyValue::Array(OtlpArrayValue {
                    values: Some(vec![OtlpAnyValue::String("a".to_string())]),
                }),
            },
        ]),
    });
    assert_eq!(
        otlp_value_to_json(&otlp_value),
        json!({"items": ["a"], "ok": true})
    );

    let proto_value = ProtoAnyValue {
        value: Some(proto_any_value::Value::KvlistValue(
            opentelemetry_proto::tonic::common::v1::KeyValueList {
                values: vec![ProtoKeyValue {
                    key: "answer".to_string(),
                    value: Some(ProtoAnyValue {
                        value: Some(proto_any_value::Value::IntValue(42)),
                    }),
                    ..Default::default()
                }],
            },
        )),
    };
    assert_eq!(proto_value_to_json(&proto_value), json!({"answer": 42}));
}
