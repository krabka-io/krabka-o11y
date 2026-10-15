use super::*;
use krabka_traceql::collect_span_field_values;
use krabka_traceql::collect_event_values;
use krabka_traceql::collect_link_values;

pub(crate) fn add_intrinsic_tags(
    mut tags: Vec<ScopedTag>,
    scope: Option<TagScope>,
) -> Vec<ScopedTag> {
    if matches!(scope, None | Some(TagScope::Intrinsic)) {
        merge_static_scope(&mut tags, TagScope::Intrinsic, INTRINSIC_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Event)) {
        merge_static_scope(&mut tags, TagScope::Event, EVENT_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Link)) {
        merge_static_scope(&mut tags, TagScope::Link, LINK_TAGS);
    }
    if matches!(scope, None | Some(TagScope::Instrumentation)) {
        merge_static_scope(&mut tags, TagScope::Instrumentation, INSTRUMENTATION_TAGS);
    }
    tags
}

pub(crate) fn collect_span_intrinsic_values(
    span: &SpanRef,
    spans: &[SpanRef],
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) {
    match tag {
        "span:childCount" => {
            let count = spans
                .iter()
                .filter(|other| other.nested_set_parent == span.nested_set_left)
                .count();
            values.insert(TypedValue::new("int", count.to_string()));
        }
        "span:nestedSetLeft" => {
            values.insert(TypedValue::new("int", span.nested_set_left.to_string()));
        }
        "span:nestedSetParent" | "span:Parent" => {
            values.insert(TypedValue::new("int", span.nested_set_parent.to_string()));
        }
        "span:nestedSetRight" => {
            values.insert(TypedValue::new("int", span.nested_set_right.to_string()));
        }
        _ => collect_span_field_values(span.into(), tag, values),
    }
}

pub(crate) fn collect_trace_intrinsic_values(
    trace: &TraceSpans,
    tag: &str,
    values: &mut BTreeSet<TypedValue>,
) {
    match tag {
        "trace:duration" => {
            if let Some(duration) = trace_duration(trace) {
                values.insert(TypedValue::new(
                    "duration",
                    duration.nanos_i64().to_string(),
                ));
            }
        }
        "trace:id" => {
            values.insert(TypedValue::new("string", hex::encode(trace.trace_id)));
        }
        "trace:rootName" => {
            values.insert(TypedValue::new("string", trace.root_trace_name.clone()));
        }
        "trace:rootService" => {
            values.insert(TypedValue::new("string", trace.root_service_name.clone()));
        }
        _ => {}
    }
}

pub(crate) fn dedup_attrs(attrs_in: &[Attribute], fallback_service_name: &str) -> Vec<Attribute> {
    let mut seen = BTreeSet::new();
    let mut attrs = Vec::new();
    for attribute in attrs_in {
        seen.insert(attribute.key.clone());
        attrs.push(attribute.clone());
    }
    if !fallback_service_name.is_empty() && seen.insert("service.name".into()) {
        attrs.push(Attribute::new(
            "service.name",
            AttrValue::Str(fallback_service_name.to_string()),
        ));
    }
    attrs
}

/// Group spans by their instrumentation scope's name, version, and
/// attributes, in the order each scope first appears.
pub(crate) fn group_by_instrumentation(input_spans: Vec<&SpanRef>) -> InstrumentationGroups<'_> {
    let mut groups: InstrumentationGroups<'_> = Vec::new();
    for span in input_spans {
        let key = InstrumentationKey {
            name: span.instrumentation_name.clone(),
            version: span.instrumentation_version.clone(),
            attributes: instrumentation_attributes(span),
        };
        if let Some(group) = groups.iter_mut().find(|group| group.key == key) {
            group.spans.push(span);
        } else {
            groups.push(InstrumentationGroup {
                key,
                spans: vec![span],
            });
        }
    }
    groups
}

pub(crate) async fn matching_traces<S>(
    engine: &TraceqlEngine<S>,
    tenant: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    scan_options: ScanOptions,
    limit: usize,
) -> Result<Vec<TraceSpans>, TraceqlError>
where
    S: SpanStore + 'static,
{
    let resp = engine
        .search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                spss: 0,
                search_limit: Some(limit),
                scan_options,
            },
        )
        .await?;
    let mut seen = BTreeSet::new();
    let mut traces = Vec::new();
    for trace in resp.traces {
        if seen.insert(trace.trace_id)
            && let Some(trace) = engine
                .trace_by_id_within(tenant, &trace.trace_id, start_ns, end_ns)
                .await?
        {
            traces.push(trace);
        }
    }
    Ok(traces)
}

pub(crate) fn merge_static_scope(tags: &mut Vec<ScopedTag>, scope: TagScope, static_tags: &[&str]) {
    let existing = tags
        .iter()
        .position(|scoped| scoped.scope == scope)
        .map_or_else(Default::default, |idx| tags.remove(idx).tags);
    let mut seen = BTreeSet::new();
    let mut merged = Vec::new();
    for tag in static_tags {
        if seen.insert((*tag).to_string()) {
            merged.push((*tag).to_string());
        }
    }
    for tag in existing {
        if seen.insert(tag.clone()) {
            merged.push(tag);
        }
    }
    tags.push(ScopedTag {
        scope,
        tags: merged,
    });
}

pub(crate) fn scoped_tags_from_traces(
    traces: &[TraceSpans],
    scope: Option<TagScope>,
) -> Vec<ScopedTag> {
    let mut out = Vec::new();

    if matches!(scope, None | Some(TagScope::Resource)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            tags.extend(
                trace_resource_attributes(trace)
                    .into_iter()
                    .map(|attribute| attribute.key),
            );
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Resource,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Span)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                tags.extend(
                    span.attributes
                        .iter()
                        .map(|attribute| attribute.key.clone()),
                );
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Span,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Event)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                for event in &span.events {
                    tags.extend(
                        event
                            .attributes
                            .iter()
                            .map(|attribute| attribute.key.clone()),
                    );
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Event,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Link)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                for link in &span.links {
                    tags.extend(
                        link.attributes
                            .iter()
                            .map(|attribute| attribute.key.clone()),
                    );
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Link,
                tags: tags.into_iter().collect(),
            });
        }
    }

    if matches!(scope, None | Some(TagScope::Instrumentation)) {
        let mut tags = BTreeSet::new();
        for trace in traces {
            for span in &trace.spans {
                if !span.instrumentation_name.is_empty() {
                    tags.insert("instrumentation:name".to_string());
                }
                if !span.instrumentation_version.is_empty() {
                    tags.insert("instrumentation:version".to_string());
                }
            }
        }
        if !tags.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Instrumentation,
                tags: tags.into_iter().collect(),
            });
        }
    }

    out
}

pub(crate) async fn search<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_inner(&request).await;
    request.state.record_query("search", resp.status(), start);
    resp
}

pub(crate) async fn search_inner<S>(request: &QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let QuerierRequest {
        state,
        principal,
        headers,
        uri,
    } = request;
    let tenant = match request_tenant(headers, principal, &state.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    let query = match search_query(uri) {
        Ok(Some(query)) => query,
        Ok(None) => {
            return status_text_response(StatusCode::BAD_REQUEST, "missing query parameter q");
        }
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let bounds = match required_time_range(uri) {
        Ok(range) => range,
        Err(rejection) => return *rejection,
    };
    let limit = match optional_usize_param(uri, "limit") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let limits = state.cfg.limits_for_tenant(&tenant);
    if let Err(err) =
        QueryEnforcer::check_search_limit(&limits, u64::try_from(limit).unwrap_or(u64::MAX))
    {
        return limit_error_response(&err);
    }
    if let Err(err) = QueryEnforcer::check_search_duration(&limits, bounds.start_ns, bounds.end_ns)
    {
        return limit_error_response(&err);
    }
    if limit > state.engine.max_traces() {
        return status_text_response(
            StatusCode::TOO_MANY_REQUESTS,
            "max traces per search exceeded",
        );
    }
    let spss = match optional_usize_param(uri, "spss") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let min_duration = match duration_param(uri, "minDuration") {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let max_duration = match duration_param(uri, "maxDuration") {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };
    let scan_options = match scan_options_param(uri) {
        Ok(value) => value,
        Err(err) => return status_text_response(StatusCode::BAD_REQUEST, err),
    };

    let duration_filtered = min_duration.is_some() || max_duration.is_some();
    let search_limit = duration_filtered.then_some(usize::MAX);

    match state
        .engine
        .search_with_options(
            tenant.as_str(),
            &query,
            bounds.start_ns,
            bounds.end_ns,
            SearchOptions {
                limit,
                spss,
                search_limit,
                scan_options,
            },
        )
        .await
    {
        Ok(resp) => {
            let resp = if duration_filtered {
                filter_search_duration(
                    resp,
                    min_duration,
                    max_duration,
                    state.engine.effective_search_limit(limit),
                )
            } else {
                resp
            };
            Json(search_json(resp)).into_response()
        }
        Err(err) => status_text_response(StatusCode::BAD_REQUEST, err.to_string()),
    }
}

pub(crate) fn search_query(uri: &Uri) -> Result<Option<String>, &'static str> {
    if let Some(query) = query_param(uri, "q") {
        return Ok(Some(query));
    }
    query_param(uri, "tags")
        .map(|tags| tags_to_traceql(&tags).ok_or("invalid query parameter tags"))
        .transpose()
}

pub(crate) async fn search_stream<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let response = search_inner(&request).await;
    let status = response.status();
    let Ok(mut bytes) = axum::body::to_bytes(response.into_body(), usize::MAX).await else {
        return Response::builder()
            .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            .body(axum::body::Body::from("response body failed"))
            .expect("the HTTP response builds");
    };
    if status.is_success() {
        bytes = [bytes.as_ref(), b"\n"].concat().into();
    }
    Response::builder()
        .status(status)
        .header("content-type", "application/x-ndjson")
        .body(axum::body::Body::from(bytes))
        .expect("the HTTP response builds")
}

pub(crate) async fn search_tag_values<S>(
    request: QuerierRequest<S>,
    Path(tag): Path<String>,
) -> Response
where
    S: SpanStore + 'static,
{
    timed_tag_values(request, tag, search_tag_values_json).await
}

pub(crate) fn search_tag_values_json(values: &[TypedValue]) -> Value {
    json!({
        "tagValues": values.iter().map(|value| &value.value).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

pub(crate) async fn search_tag_values_v2<S>(
    request: QuerierRequest<S>,
    Path(tag): Path<String>,
) -> Response
where
    S: SpanStore + 'static,
{
    timed_tag_values(request, tag, search_tag_values_v2_json).await
}

pub(crate) fn search_tag_values_v2_json(values: &[TypedValue]) -> Value {
    json!({
        "tagValues": values.iter().map(|value| {
            json!({
                "type": &value.type_,
                "value": &value.value,
            })
        }).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

pub(crate) async fn search_tags<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tags_inner(&request, |tags, _| search_tags_json(&tags)).await;
    request.state.record_query("tags", resp.status(), start);
    resp
}

pub(crate) fn search_tags_json(tags: &[ScopedTag]) -> Value {
    json!({
        "tagNames": tags.iter().flat_map(|scope| scope.tags.iter()).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

pub(crate) async fn search_tags_v2<S>(request: QuerierRequest<S>) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tags_inner(&request, |tags, scope| {
        search_tags_v2_json(&add_intrinsic_tags(tags, scope))
    })
    .await;
    request.state.record_query("tags", resp.status(), start);
    resp
}

pub(crate) fn search_tags_v2_json(tags: &[ScopedTag]) -> Value {
    json!({
        "scopes": tags.iter().map(|scope| {
            json!({
                "name": tag_scope_name(scope.scope),
                "tags": &scope.tags,
            })
        }).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}

pub(crate) fn tag_values_from_traces(traces: &[TraceSpans], tag: &str) -> Vec<TypedValue> {
    let tag = tag.strip_prefix('.').unwrap_or(tag);
    let prefix = TagScope::split_resource_or_span_prefix(tag);
    let attr_tag = prefix.tag;
    let attr_scope = prefix.scope;
    let mut values = BTreeSet::new();
    for trace in traces {
        collect_trace_intrinsic_values(trace, tag, &mut values);
        if matches!(attr_scope, None | Some(TagScope::Resource)) {
            values.extend(
                trace_resource_attributes(trace)
                    .into_iter()
                    .filter(|attribute| attribute.key == attr_tag)
                    .flat_map(|attribute| typed_value_parts(&attribute.value)),
            );
        }
        for span in &trace.spans {
            collect_span_intrinsic_values(span, &trace.spans, tag, &mut values);
            collect_event_values(&span.events, tag, &mut values);
            collect_link_values(&span.links, tag, &mut values);
            if matches!(attr_scope, None | Some(TagScope::Span)) {
                values.extend(
                    span.attributes
                        .iter()
                        .filter(|attribute| attribute.key == attr_tag)
                        .flat_map(|attribute| typed_value_parts(&attribute.value)),
                );
            }
        }
    }
    values.into_iter().collect()
}

pub(crate) fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|tag| {
            // The key becomes an unquoted TraceQL attribute reference, so a key
            // carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys
            // rather than interpolating their raw bytes.
            key_is_safe_attribute(&tag.key).then(|| {
                format!(
                    "{} = \"{}\"",
                    traceql_tag_field(&tag.key),
                    tag.value.replace('\\', "\\\\").replace('"', "\\\"")
                )
            })
        })
        .collect::<Option<Vec<_>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}

/// Answers a tag-values request for `tag`, rendering the values with
/// `render`, and records the query under `tag_values`.
pub(crate) async fn timed_tag_values<S>(
    request: QuerierRequest<S>,
    tag: String,
    render: fn(&[TypedValue]) -> Value,
) -> Response
where
    S: SpanStore + 'static,
{
    let start = std::time::Instant::now();
    let resp = search_tag_values_inner(&request, TagValuesRequest { tag, render }).await;
    request
        .state
        .record_query("tag_values", resp.status(), start);
    resp
}
