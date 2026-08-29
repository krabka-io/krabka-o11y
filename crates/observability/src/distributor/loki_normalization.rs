use crate::{
    CONTENT_TYPE, DistributorError, HeaderMap, Labels, LokiProtoPushRequest, LokiTypedPushRequest,
    MatchOp, OtlpLogsRequest, Time, Value, WalLogRecord, current_unix_time_ns,
    discover_detected_level_label, discover_service_name_label, loki_decode_error_context,
    loki_missing_proto_timestamp_error, loki_proto_label_pairs_to_labels, loki_proto_timestamp_ns,
    loki_stale_sample_label_set, otlp_attributes_to_labels, otlp_log_record_structured_metadata,
    otlp_timestamp_ns, otlp_value_to_string, parse_query, parse_structured_metadata,
    quote_logql_string, tenant, validate_ingest_timestamp_ns, validate_loki_timestamp_window,
};
use krabka_units::convert::TimeExt;
pub(crate) fn is_protobuf_content_type(headers: &HeaderMap) -> bool {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    content_type.split(';').next().is_some_and(|content_type| {
        matches!(
            content_type.trim(),
            "application/x-protobuf" | "application/protobuf"
        )
    })
}

pub(crate) fn is_loki_json_content_type(headers: &HeaderMap) -> Result<bool, DistributorError> {
    let Some(content_type) = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    else {
        return Ok(false);
    };
    let content_type = content_type.trim();
    if content_type.is_empty() {
        return Ok(false);
    }

    let mut parts = content_type.split(';');
    let media_type = parts.next().unwrap_or_default().trim();
    if media_type.is_empty() {
        return Err(DistributorError::InvalidLokiContentType(
            content_type.to_string(),
        ));
    }

    let mut parameters = parts.peekable();
    while let Some(parameter) = parameters.next() {
        let parameter = parameter.trim();
        if parameter.is_empty() && parameters.peek().is_none() {
            continue;
        }
        let Some((name, value)) = parameter.split_once('=') else {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        };
        if name.trim().is_empty() || value.trim().is_empty() {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        }
    }

    Ok(media_type.eq_ignore_ascii_case("application/json"))
}

pub(crate) fn normalize_loki_push(
    headers: &HeaderMap,
    payload: LokiTypedPushRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for stream in payload.streams {
        let Some(original_stream_labels) = stream.stream else {
            continue;
        };
        validate_loki_stream_labels(&original_stream_labels)?;
        let mut stream_labels = original_stream_labels.clone();
        discover_service_name_label(&mut stream_labels);

        let Some(values) = stream.values else {
            continue;
        };
        for value in values {
            let Some(value) = value.as_array() else {
                return Err(DistributorError::InvalidPushValue);
            };
            let zero_timestamp;
            let (timestamp, line, metadata, is_empty_value) = match value.as_slice() {
                [timestamp] => (timestamp, "", [].as_slice(), false),
                [timestamp, line, metadata @ ..] => (
                    timestamp,
                    line.as_str().ok_or_else(|| {
                        DistributorError::InvalidJsonLineSyntax(loki_json_line_parse_error(
                            &original_stream_labels,
                            timestamp.as_str().unwrap_or_default(),
                            line,
                        ))
                    })?,
                    metadata,
                    false,
                ),
                [] => {
                    zero_timestamp = Value::String("0".to_string());
                    (&zero_timestamp, "", [].as_slice(), true)
                }
            };
            let timestamp = timestamp
                .as_str()
                .ok_or(DistributorError::InvalidTimestamp)?;
            let timestamp_ns = timestamp.parse().map_err(|_| {
                DistributorError::InvalidJsonTimestampSyntax(loki_json_timestamp_parse_error(
                    timestamp, line,
                ))
            })?;
            let timestamp_ns = validate_ingest_timestamp_ns(timestamp_ns)?;
            if is_empty_value {
                validate_loki_empty_json_value_timestamp_window(
                    &stream_labels,
                    reject_old_samples_max_age,
                )?;
            }
            validate_loki_timestamp_window(
                timestamp_ns,
                &stream_labels,
                reject_old_samples_max_age,
                creation_grace_period,
            )?;
            let labels = loki_push_entry_labels(&stream_labels, line);

            records.push(WalLogRecord {
                tenant: tenant.clone(),
                labels,
                timestamp_ns,
                line: line.to_string(),
                structured_metadata: parse_structured_metadata(metadata.first())?,
                position: None,
            });
        }
    }

    Ok(records)
}

pub(crate) fn validate_loki_empty_json_value_timestamp_window(
    stream_labels: &Labels,
    max_age: Option<Time>,
) -> Result<(), DistributorError> {
    let Some(max_age) = max_age else {
        return Ok(());
    };
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    Err(DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    })
}

pub(crate) fn normalize_loki_proto_push(
    headers: &HeaderMap,
    payload: LokiProtoPushRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for stream in payload.streams {
        let mut stream_labels = parse_loki_proto_labels(&stream.labels)?;
        validate_loki_stream_labels(&stream_labels)?;
        discover_service_name_label(&mut stream_labels);

        for entry in stream.entries {
            let timestamp_ns = if let Some(timestamp) = entry.timestamp.as_ref() {
                loki_proto_timestamp_ns(Some(timestamp))?
            } else {
                return Err(loki_missing_proto_timestamp_error(
                    &stream_labels,
                    reject_old_samples_max_age,
                ));
            };
            validate_loki_timestamp_window(
                timestamp_ns,
                &stream_labels,
                reject_old_samples_max_age,
                creation_grace_period,
            )?;
            let labels = loki_push_entry_labels(&stream_labels, &entry.line);
            records.push(WalLogRecord {
                tenant: tenant.clone(),
                labels,
                timestamp_ns,
                line: entry.line,
                structured_metadata: loki_proto_label_pairs_to_labels(&entry.structured_metadata),
                position: None,
            });
        }
    }

    if records.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }

    Ok(records)
}

pub(crate) fn loki_push_entry_labels(stream_labels: &Labels, line: &str) -> Labels {
    let mut labels = stream_labels.clone();
    discover_detected_level_label(&mut labels, line);
    labels
}

pub(crate) fn normalize_otlp_logs(
    headers: &HeaderMap,
    payload: OtlpLogsRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_labels = otlp_attributes_to_labels(
            resource_logs
                .resource
                .as_ref()
                .and_then(|resource| resource.attributes.as_deref()),
        )?;

        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            labels.extend(otlp_attributes_to_labels(
                scope_logs
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.attributes.as_deref()),
            )?);
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }

            for log_record in scope_logs.log_records {
                let timestamp_ns = otlp_timestamp_ns(&log_record.time_unix_nano)?;
                validate_loki_timestamp_window(
                    timestamp_ns,
                    &labels,
                    reject_old_samples_max_age,
                    creation_grace_period,
                )?;
                records.push(WalLogRecord {
                    tenant: tenant.clone(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(otlp_value_to_string)
                        .unwrap_or_default(),
                    structured_metadata: otlp_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}

pub(crate) fn loki_json_timestamp_parse_error(timestamp: &str, line: &str) -> String {
    let found_context = timestamp
        .char_indices()
        .nth(9)
        .map_or(timestamp, |(offset, _)| &timestamp[offset..]);
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}\"]]}}]}}|..., bigger context ...|s\":[[\"{timestamp}\",\"{line}\"]]}}]}}|...\n"
    )
}

pub(crate) fn loki_json_timestamp_value_parse_error(
    body: &[u8],
    timestamp: &Value,
    line: Option<&Value>,
) -> String {
    let body = String::from_utf8_lossy(body);
    let timestamp_text = timestamp.to_string();
    let value_start = body.find(&timestamp_text).unwrap_or(body.len());
    let found_context = line.and_then(Value::as_str).map_or_else(
        || loki_decode_error_context(&body, value_start.saturating_add(10)).to_string(),
        |line| {
            let start = line
                .char_indices()
                .nth(line.chars().count().saturating_sub(6))
                .map_or(0, |(offset, _)| offset);
            format!("{}\"]]}}]}}", &line[start..])
        },
    );
    let context_prefix_len = if timestamp.is_array() {
        10
    } else if timestamp.is_object() {
        4
    } else {
        9
    };
    let bigger_context =
        loki_decode_error_context(&body, value_start.saturating_sub(context_prefix_len));

    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}|..., bigger context ...|{bigger_context}|...\n"
    )
}

pub(crate) fn loki_json_line_parse_error(
    stream_labels: &Labels,
    timestamp: &str,
    line: &Value,
) -> String {
    let line = line.to_string();
    let found_context = format!(
        "{}\",{}]]}}]}}",
        timestamp
            .char_indices()
            .nth(timestamp.chars().count().saturating_sub(2))
            .map_or(timestamp, |(offset, _)| &timestamp[offset..]),
        line
    );
    let labels = serde_json::to_string(stream_labels).unwrap_or_else(|_| "{}".to_string());
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|{found_context}|..., bigger context ...|ream\":{labels},\"values\":[[\"{timestamp}\",{line}]]}}]}}|...\n"
    )
}

pub(crate) fn validate_loki_stream_labels(labels: &Labels) -> Result<(), DistributorError> {
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(DistributorError::InvalidPushLabelSyntax(
            loki_push_label_parse_error(labels, name),
        ));
    }
    Ok(())
}

pub(crate) fn loki_push_label_parse_error(labels: &Labels, invalid_name: &str) -> String {
    let rendered = loki_label_set(labels);
    let name_start = rendered.find(invalid_name).unwrap_or(1);
    let invalid_offset = invalid_name
        .char_indices()
        .find_map(|(offset, value)| {
            (!is_loki_label_name_char(value, offset == 0)).then_some(offset)
        })
        .unwrap_or(0);
    let column = name_start + invalid_offset + 1;
    let unexpected = invalid_name[invalid_offset..].chars().next().unwrap_or('}');
    format!(
        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{unexpected}'\n"
    )
}

pub(crate) fn loki_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quote_logql_string(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{values}}}")
}

pub(crate) fn is_loki_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    is_loki_label_name_char(first, true) && chars.all(|value| is_loki_label_name_char(value, false))
}

pub(crate) fn is_loki_label_name_char(value: char, first: bool) -> bool {
    value == '_' || value.is_ascii_alphabetic() || (!first && value.is_ascii_digit())
}

pub(crate) fn parse_loki_proto_labels(labels: &str) -> Result<Labels, DistributorError> {
    let labels = labels.trim();
    if labels.is_empty() || labels == "{}" {
        return Ok(Labels::new());
    }

    let query = parse_query(labels).map_err(|_| {
        loki_proto_label_parse_error(labels).map_or(
            DistributorError::InvalidPushLabels,
            DistributorError::InvalidPushLabelSyntax,
        )
    })?;
    if !query.pipeline.is_empty() {
        return Err(DistributorError::InvalidPushLabels);
    }

    let mut labels = Labels::new();
    let mut rendered_labels = Vec::new();
    for matcher in query.matchers {
        if matcher.op != MatchOp::Equal {
            return Err(DistributorError::InvalidPushLabels);
        }
        rendered_labels.push(format!(
            "{}={}",
            matcher.name,
            quote_logql_string(&matcher.value)
        ));
        if labels.contains_key(&matcher.name) {
            let mut discovered_labels = labels.clone();
            discover_service_name_label(&mut discovered_labels);
            if !rendered_labels
                .iter()
                .any(|label| label.starts_with("service_name="))
                && let Some(service_name) = discovered_labels.get("service_name")
            {
                rendered_labels.push(format!("service_name={}", quote_logql_string(service_name)));
            }
            return Err(DistributorError::InvalidPushLabelSyntax(format!(
                "stream '{{{}}}' has duplicate label name: '{}'\n",
                rendered_labels.join(", "),
                matcher.name
            )));
        }
        labels.insert(matcher.name, matcher.value);
    }

    Ok(labels)
}

pub(crate) fn loki_proto_label_parse_error(labels: &str) -> Option<String> {
    let labels = labels.trim();
    let mut chars = labels.char_indices();
    if chars.next()? != (0, '{') {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut expecting_name = true;
    let mut first_name_char = true;

    for (offset, value) in chars {
        if in_string {
            if escaped {
                escaped = false;
            } else if value == '\\' {
                escaped = true;
            } else if value == '"' {
                in_string = false;
            }
            continue;
        }

        match value {
            '"' => in_string = true,
            ',' => {
                expecting_name = true;
                first_name_char = true;
            }
            // No `first_name_char` here: nothing reads it until a `,` starts
            // the next name, and that arm sets it itself.
            '=' => expecting_name = false,
            '}' => break,
            value if expecting_name && value.is_whitespace() => {}
            value if expecting_name => {
                if !is_loki_label_name_char(value, first_name_char) {
                    let column = labels[..offset].chars().count() + 1;
                    return Some(format!(
                        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{value}'\n"
                    ));
                }
                first_name_char = false;
            }
            _ => {}
        }
    }

    None
}
