use krabka_units::convert::{ByteSizeExt, TimeExt};

use crate::{
    ByteSize, CONTENT_TYPE, DistributorError, HeaderMap, Labels, Limits, LokiProtoPushRequest,
    LokiTypedPushRequest, MatchOp, OtlpLogsRequest, TenantId, Time, Value, WalLogRecord,
    current_unix_time_ns, discover_detected_level_label, discover_service_name_label,
    distributor::otlp_normalization::{
        OtlpLine, OtlpScope, group_otlp_resource, group_otlp_scope, normalize_log_hierarchy,
    },
    loki_decode_error_context, loki_missing_proto_timestamp_error,
    loki_proto_label_pairs_to_labels, loki_proto_timestamp_ns, loki_stale_sample_label_set,
    otlp_log_record_structured_metadata, otlp_timestamp_ns, otlp_value_to_string, parse_query,
    parse_structured_metadata, quote_logql_string, validate_ingest_timestamp_ns,
    validate_loki_timestamp_window,
};

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

pub(crate) fn loki_json_timestamp_parse_error(timestamp: &str, line: &str) -> String {
    let found_context = timestamp
        .char_indices()
        .nth(9)
        .map_or(timestamp, |character| &timestamp[character.0..]);
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|{found_context}\"]]}}]}}|..., bigger context ...|s\":[[\"{timestamp}\",\"{line}\"]]}}]}}|...\n"
    )
}

pub(crate) fn loki_label_set(labels: &Labels) -> String {
    crate::distributor::format_loki_label_set(labels, ",")
}

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

pub(crate) fn loki_push_label_parse_error(labels: &Labels, invalid_name: &str) -> String {
    let rendered = loki_label_set(labels);
    let name_start = rendered.find(invalid_name).unwrap_or(1);
    let invalid_offset = invalid_name
        .char_indices()
        .find_map(|character| {
            (!is_loki_label_name_char(character.1, character.0 == 0)).then_some(character.0)
        })
        .unwrap_or(0);
    let column = name_start + invalid_offset + 1;
    let unexpected = invalid_name[invalid_offset..].chars().next().unwrap_or('}');
    format!(
        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{unexpected}'\n"
    )
}

pub(crate) fn validate_loki_empty_json_value_timestamp_window(
    stream_labels: &Labels,
    max_age: Time,
) -> Result<(), DistributorError> {
    if max_age <= Time::ZERO {
        return Ok(());
    }
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    Err(DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    })
}

/// Applies `Loki`'s `max_line_size` to one entry.
///
/// The size is the line's UTF-8 byte count, which is what `Loki` measures.
/// Krabka always discards an oversize line: `Loki`'s
/// `max_line_size_truncate` cuts the line instead, and Krabka does not
/// implement that switch, so an operator who wants a line kept must raise the
/// cap.
pub(crate) fn validate_loki_line_size(
    line: &str,
    stream_labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    if limits.max_line_size <= ByteSize::ZERO {
        return Ok(());
    }
    let limit_bytes = limits.max_line_size.bytes_usize();
    if line.len() <= limit_bytes {
        return Ok(());
    }
    Err(DistributorError::LineTooLong {
        stream: loki_stale_sample_label_set(stream_labels),
        limit_bytes,
        line_bytes: line.len(),
    })
}

/// Checks a pushed stream's labels for `Loki`'s name syntax and then for the
/// tenant's three label caps.
///
/// Syntax first, as `Loki` does: a label name that is not a name at all is not
/// a limit failure, and its message names the offending character.
pub(crate) fn validate_loki_stream_labels(
    labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(DistributorError::InvalidPushLabelSyntax(
            loki_push_label_parse_error(labels, name),
        ));
    }
    validate_loki_label_limits(labels, limits)
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
        let Some(parts) = parameter.split_once('=') else {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        };
        let name = parts.0;
        let value = parts.1;
        if name.trim().is_empty() || value.trim().is_empty() {
            return Err(DistributorError::InvalidLokiContentType(
                content_type.to_string(),
            ));
        }
    }

    Ok(media_type.eq_ignore_ascii_case("application/json"))
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
                .map_or(0, |character| character.0);
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

pub(crate) fn loki_proto_label_parse_error(labels: &str) -> Option<String> {
    let labels = labels.trim();
    let mut chars = labels.char_indices();
    let first = chars.next()?;
    if first.0 != 0 || first.1 != '{' {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut expecting_name = true;
    let mut first_name_char = true;

    for character in chars {
        let offset = character.0;
        let value = character.1;
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

pub(crate) fn normalize_loki_proto_push(
    tenant: &TenantId,
    payload: LokiProtoPushRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for stream in payload.streams {
        let mut stream_labels = parse_loki_proto_labels(&stream.labels)?;
        validate_loki_stream_labels(&stream_labels, limits)?;
        discover_service_name_label(&mut stream_labels);

        for mut entry in stream.entries {
            if entry
                .structured_metadata
                .iter()
                .any(|label| label.name.is_empty())
            {
                return Err(DistributorError::EmptyStructuredMetadataLabelName);
            }
            let timestamp_ns = if let Some(timestamp) = entry.timestamp.as_ref() {
                loki_proto_timestamp_ns(Some(timestamp))?
            } else {
                return Err(loki_missing_proto_timestamp_error(
                    &stream_labels,
                    limits.reject_old_samples_max_age,
                ));
            };
            validate_loki_timestamp_window(timestamp_ns, &stream_labels, limits)?;
            truncate_loki_line(&mut entry.line, limits);
            validate_loki_line_size(&entry.line, &stream_labels, limits)?;
            let labels = stream_labels.clone();
            let mut structured_metadata =
                loki_proto_label_pairs_to_labels(&entry.structured_metadata);
            validate_structured_metadata_limits(&structured_metadata, &stream_labels, limits)?;
            discover_detected_level_label(
                &stream_labels,
                &mut structured_metadata,
                &entry.line,
                limits,
            );
            records.push(WalLogRecord {
                tenant: tenant.to_owned(),
                labels,
                timestamp_ns,
                line: entry.line,
                structured_metadata,
                position: None,
            });
        }
    }

    if records.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }

    Ok(records)
}

struct ParsedPushValue<'a> {
    timestamp: &'a Value,
    line: &'a str,
    metadata: &'a [Value],
    is_empty: bool,
}

pub(crate) fn normalize_loki_push(
    tenant: &TenantId,
    payload: LokiTypedPushRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for stream in payload.streams {
        let Some(original_stream_labels) = stream.stream else {
            continue;
        };
        validate_loki_stream_labels(&original_stream_labels, limits)?;
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
            let parsed = match value.as_slice() {
                [timestamp] => ParsedPushValue {
                    timestamp,
                    line: "",
                    metadata: [].as_slice(),
                    is_empty: false,
                },
                [timestamp, line, metadata @ ..] => ParsedPushValue {
                    timestamp,
                    line: line.as_str().ok_or_else(|| {
                        DistributorError::InvalidJsonLineSyntax(loki_json_line_parse_error(
                            &original_stream_labels,
                            timestamp.as_str().unwrap_or_default(),
                            line,
                        ))
                    })?,
                    metadata,
                    is_empty: false,
                },
                [] => {
                    zero_timestamp = Value::String("0".to_string());
                    ParsedPushValue {
                        timestamp: &zero_timestamp,
                        line: "",
                        metadata: [].as_slice(),
                        is_empty: true,
                    }
                }
            };
            let timestamp = parsed
                .timestamp
                .as_str()
                .ok_or(DistributorError::InvalidTimestamp)?;
            let timestamp_ns = timestamp.parse().map_err(|_| {
                DistributorError::InvalidJsonTimestampSyntax(loki_json_timestamp_parse_error(
                    timestamp,
                    parsed.line,
                ))
            })?;
            let timestamp_ns = validate_ingest_timestamp_ns(timestamp_ns)?;
            if parsed.is_empty {
                validate_loki_empty_json_value_timestamp_window(
                    &stream_labels,
                    limits.reject_old_samples_max_age,
                )?;
            }
            validate_loki_timestamp_window(timestamp_ns, &stream_labels, limits)?;
            let mut line = parsed.line.to_string();
            truncate_loki_line(&mut line, limits);
            validate_loki_line_size(&line, &stream_labels, limits)?;
            let labels = stream_labels.clone();

            let mut structured_metadata = parse_structured_metadata(parsed.metadata.first())?;
            validate_structured_metadata_limits(&structured_metadata, &stream_labels, limits)?;
            discover_detected_level_label(&stream_labels, &mut structured_metadata, &line, limits);
            records.push(WalLogRecord {
                tenant: tenant.to_owned(),
                labels,
                timestamp_ns,
                line,
                structured_metadata,
                position: None,
            });
        }
    }

    Ok(records)
}

pub(crate) fn normalize_otlp_logs(
    tenant: &TenantId,
    payload: OtlpLogsRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    normalize_log_hierarchy(
        tenant,
        payload.resource_logs,
        limits,
        |resource_logs, limits| {
            group_otlp_resource(
                resource_logs
                    .resource
                    .as_ref()
                    .and_then(|resource| resource.attributes.as_deref())
                    .unwrap_or_default(),
                resource_logs.scope_logs,
                limits,
            )
        },
        |scope_logs, resource_labels, resource_metadata, limits| {
            group_otlp_scope(
                resource_labels,
                resource_metadata,
                &OtlpScope::from_json(scope_logs.scope.as_ref()),
                scope_logs.log_records,
                limits,
            )
        },
        |log_record, labels, inherited, tenant, limits| {
            let zero_timestamp = log_record.time_unix_nano.is_null()
                || log_record.time_unix_nano.as_i64() == Some(0)
                || log_record.time_unix_nano.as_str() == Some("0");
            let timestamp = if zero_timestamp {
                log_record
                    .observed_time_unix_nano
                    .as_ref()
                    .unwrap_or(&log_record.time_unix_nano)
            } else {
                &log_record.time_unix_nano
            };
            OtlpLine {
                tenant,
                labels,
                inherited_metadata: inherited,
                timestamp_ns: otlp_timestamp_ns(timestamp)?,
                line: log_record
                    .body
                    .as_ref()
                    .map_or_else(Default::default, otlp_value_to_string),
                attributes: log_record.attributes.as_deref().unwrap_or_default(),
            }
            .into_wal_record(|| otlp_log_record_structured_metadata(&log_record), limits)
        },
    )
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

pub(crate) fn truncate_loki_line(line: &mut String, limits: &Limits) {
    if !limits.max_line_size_truncate || limits.max_line_size <= ByteSize::ZERO {
        return;
    }
    let mut end = limits.max_line_size.bytes_usize().min(line.len());
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line.truncate(end);
}

#[cfg(test)]
mod tests {
    use krabka_units::bytes;

    use super::*;

    #[test]
    fn truncation_keeps_a_utf8_boundary() {
        let mut line = "abéz".to_string();
        truncate_loki_line(
            &mut line,
            &Limits {
                max_line_size: bytes(3),
                max_line_size_truncate: true,
                ..Limits::unenforced()
            },
        );
        assert_eq!(line, "ab");
    }
}

/// Applies `Loki`'s three per-stream label caps.
///
/// `Loki` checks the label count, then each name's length, then each value's
/// length, and reports the first that fails. The messages are `Loki`'s own, so
/// a client that matches on them keeps working.
pub(crate) fn validate_loki_label_limits(
    labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    if limits.max_label_names_per_series > 0 {
        let observed = u64::try_from(labels.len()).unwrap_or(u64::MAX);
        if observed > limits.max_label_names_per_series {
            return Err(DistributorError::TooManyLabelNames {
                stream: loki_stale_sample_label_set(labels),
                observed,
                limit: limits.max_label_names_per_series,
            });
        }
    }
    for entry in labels {
        let name = &entry.0;
        let value = &entry.1;
        if exceeds(name.len(), limits.max_label_name_length) {
            return Err(DistributorError::LabelNameTooLong {
                stream: loki_stale_sample_label_set(labels),
                name: (*name).clone(),
            });
        }
        if exceeds(value.len(), limits.max_label_value_length) {
            return Err(DistributorError::LabelValueTooLong {
                stream: loki_stale_sample_label_set(labels),
                value: (*value).clone(),
            });
        }
    }
    Ok(())
}

fn exceeds(observed: usize, limit: ByteSize) -> bool {
    limit > ByteSize::ZERO && observed > limit.bytes_usize()
}

pub(crate) fn validate_structured_metadata_limits(
    metadata: &Labels,
    stream_labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    let observed_count = metadata.len();
    if limits.max_structured_metadata_entries_count > 0
        && u64::try_from(observed_count).unwrap_or(u64::MAX)
            > limits.max_structured_metadata_entries_count
    {
        return Err(DistributorError::TooManyStructuredMetadataLabels {
            stream: loki_stale_sample_label_set(stream_labels),
            observed: observed_count,
            limit: limits.max_structured_metadata_entries_count,
        });
    }
    let observed_size = metadata
        .iter()
        .map(|entry| entry.0.len() + entry.1.len())
        .sum();
    if limits.max_structured_metadata_size > ByteSize::ZERO
        && observed_size > limits.max_structured_metadata_size.bytes_usize()
    {
        return Err(DistributorError::StructuredMetadataTooLarge {
            stream: loki_stale_sample_label_set(stream_labels),
            observed: observed_size,
            limit: limits.max_structured_metadata_size.bytes_usize(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn metadata_count_and_byte_caps_are_enforced() {
        let mut labels = Labels::new();
        labels.insert("app".into(), "api".into());
        let mut metadata = Labels::new();
        metadata.insert("a".into(), "bc".into());
        metadata.insert("d".into(), "ef".into());
        let mut limits = Limits::unenforced();
        limits.max_structured_metadata_entries_count = 1;
        assert!(matches!(
            validate_structured_metadata_limits(&metadata, &labels, &limits),
            Err(DistributorError::TooManyStructuredMetadataLabels { .. })
        ));
        limits.max_structured_metadata_entries_count = 0;
        limits.max_structured_metadata_size = krabka_units::bytes(5);
        assert!(matches!(
            validate_structured_metadata_limits(&metadata, &labels, &limits),
            Err(DistributorError::StructuredMetadataTooLarge { .. })
        ));
    }
}
