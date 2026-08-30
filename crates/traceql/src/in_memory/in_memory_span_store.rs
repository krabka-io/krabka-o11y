use super::*;

/// In-memory span store keyed by tenant.
#[derive(Default)]
pub struct InMemorySpanStore {
    pub(crate) traces: HashMap<String, Vec<StoredTrace>>,
}

impl InMemorySpanStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_trace(
        &mut self,
        tenant: &str,
        root_service_name: &str,
        root_span_name: &str,
        spans: Vec<InputSpan>,
    ) {
        let trace_id = spans.first().map_or([0; 16], |s| s.trace_id);
        let trace_start_unix_nano = spans.iter().map(|s| s.start_unix_nano).min().unwrap_or(0);
        let trace_end_unix_nano = spans
            .iter()
            .map(|s| s.start_unix_nano + s.duration.nanos_i64())
            .max()
            .unwrap_or(trace_start_unix_nano);
        let nested = assign_nested_set(&spans);

        self.traces
            .entry(tenant.to_string())
            .or_default()
            .push(StoredTrace {
                trace_id,
                root_service_name: root_service_name.to_string(),
                root_span_name: root_span_name.to_string(),
                trace_start_unix_nano,
                trace_duration: Time::from_nanos(trace_end_unix_nano - trace_start_unix_nano),
                spans,
                nested,
            });
    }

    pub(crate) fn attr_columns(
        traces: &[&StoredTrace],
        projection_matchers: &[SpanMatcher],
    ) -> Vec<(String, DataType)> {
        let mut cols = BTreeMap::new();
        for trace in traces {
            for span in &trace.spans {
                for (key, value) in &span.attrs {
                    cols.entry(key.clone()).or_insert_with(|| match value {
                        AttrValue::Str(_) => DataType::Utf8,
                        AttrValue::Int(_) => DataType::Int64,
                        AttrValue::Float(_) => DataType::Float64,
                        AttrValue::Bool(_) => DataType::Boolean,
                    });
                }
                for matcher in projection_matchers {
                    match matcher.scope {
                        MatchScope::Event => {
                            let Some((_, value)) = span
                                .events
                                .iter()
                                .flat_map(|event| event.attributes.iter())
                                .find(|(key, _)| key == &matcher.key)
                            else {
                                continue;
                            };
                            cols.entry(format!("{EVENT_ATTR_PREFIX}{}", matcher.key))
                                .or_insert_with(|| attr_data_type(value));
                        }
                        MatchScope::Link => {
                            let Some((_, value)) = span
                                .links
                                .iter()
                                .flat_map(|link| link.attributes.iter())
                                .find(|(key, _)| key == &matcher.key)
                            else {
                                continue;
                            };
                            cols.entry(format!("{LINK_ATTR_PREFIX}{}", matcher.key))
                                .or_insert_with(|| attr_data_type(value));
                        }
                        MatchScope::Both
                        | MatchScope::Span
                        | MatchScope::Resource
                        | MatchScope::Parent
                        | MatchScope::Instrumentation
                        | MatchScope::Intrinsic => {}
                    }
                }
            }
        }
        cols.into_iter().collect()
    }
}

impl InMemorySpanStore {
    pub(crate) fn scan_with_projection(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        projection_matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult> {
        let in_range: Vec<&StoredTrace> = self
            .traces
            .get(tenant)
            .into_iter()
            .flatten()
            .filter(|trace| {
                start_ns <= trace.trace_start_unix_nano && trace.trace_start_unix_nano <= end_ns
            })
            .collect();
        let row_count: usize = in_range.iter().map(|trace| trace.spans.len()).sum();
        let attr_cols = Self::attr_columns(&in_range, projection_matchers);
        let schema = span_schema_with_attrs(&attr_cols);

        let mut builders = ScanBuilders::new(row_count);
        let mut attr_builders: Vec<(String, AttrBuilder)> = attr_cols
            .iter()
            .map(|(key, dt)| (key.clone(), AttrBuilder::new(dt)))
            .collect();

        for trace in &in_range {
            for (i, span) in trace.spans.iter().enumerate() {
                if !span_matches(trace, span, &trace.nested, i, matchers) {
                    continue;
                }
                let expansion_matchers = expansion_matchers(matchers, projection_matchers);
                let event_rows = matching_events_for_scan(span, &expansion_matchers);
                let link_rows = matching_links_for_scan(span, &expansion_matchers);
                for event in event_rows {
                    for link in &link_rows {
                        builders.append(trace, span, i, event, *link, &mut attr_builders)?;
                    }
                }
            }
        }

        let mut columns = builders.finish();
        columns.extend(attr_builders.into_iter().map(|(_, b)| b.finish()));

        let batch = RecordBatch::try_new(schema.clone(), columns)
            .map_err(|e| TraceqlError::Store(e.to_string()))?;
        let inspected =
            ByteSize::from_bytes(u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX));
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema, vec![vec![batch]])?;
        ctx.register_table("spans", Arc::new(table))?;
        Ok(ScanResult {
            ctx,
            span_table: "spans".into(),
            inspected,
        })
    }
}

#[async_trait::async_trait]
impl SpanStore for InMemorySpanStore {
    async fn scan(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<ScanResult> {
        self.scan_with_projection(tenant, matchers, &[], start_ns, end_ns)
    }

    async fn scan_with_options(
        &self,
        tenant: &str,
        matchers: &[SpanMatcher],
        start_ns: i64,
        end_ns: i64,
        options: &crate::store::ScanOptions,
    ) -> Result<ScanResult> {
        self.scan_with_projection(
            tenant,
            matchers,
            &options.projection_matchers,
            start_ns,
            end_ns,
        )
    }

    async fn trace_by_id(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>> {
        let found = self
            .traces
            .get(tenant)
            .into_iter()
            .flatten()
            .find(|trace| &trace.trace_id == trace_id);
        Ok(found.map(|trace| TraceSpans {
            trace_id: trace.trace_id,
            root_service_name: trace.root_service_name.clone(),
            root_trace_name: trace.root_span_name.clone(),
            resource_attributes: if trace.root_service_name.is_empty() {
                Vec::new()
            } else {
                vec![(
                    "service.name".to_string(),
                    AttrValue::Str(trace.root_service_name.clone()),
                )]
            },
            spans: trace
                .spans
                .iter()
                .zip(&trace.nested)
                .map(|(span, nested)| span_ref(span, nested))
                .collect(),
        }))
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        let mut resource = BTreeSet::new();
        let mut span = BTreeSet::new();
        let mut event = BTreeSet::new();
        let mut link = BTreeSet::new();
        let mut instrumentation = BTreeSet::new();
        let traces = self.traces_in_range(tenant, start_ns, end_ns);
        for trace in &traces {
            resource.insert("service.name".to_string());
            for input in &trace.spans {
                for (key, _) in &input.attrs {
                    if let Some(key) = key.strip_prefix(INSTRUMENTATION_ATTR_PREFIX) {
                        instrumentation.insert(key.to_string());
                    } else {
                        span.insert(key.clone());
                    }
                }
                if !input.events.is_empty() {
                    event.extend(EVENT_TAGS.iter().map(|tag| (*tag).to_string()));
                }
                if !input.links.is_empty() {
                    link.extend(LINK_TAGS.iter().map(|tag| (*tag).to_string()));
                }
                for event_ref in &input.events {
                    event.extend(event_ref.attributes.iter().map(|(key, _)| key.clone()));
                }
                for link_ref in &input.links {
                    link.extend(link_ref.attributes.iter().map(|(key, _)| key.clone()));
                }
                if !input.instrumentation_name.is_empty() {
                    instrumentation.insert("instrumentation:name".to_string());
                }
                if !input.instrumentation_version.is_empty() {
                    instrumentation.insert("instrumentation:version".to_string());
                }
            }
        }

        let mut out = Vec::new();
        if matches!(scope, None | Some(TagScope::Resource)) && !resource.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Resource,
                tags: resource.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Span)) && !span.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Span,
                tags: span.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Intrinsic)) && !traces.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Intrinsic,
                tags: INTRINSIC_TAGS
                    .iter()
                    .map(|tag| (*tag).to_string())
                    .collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Event)) && !event.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Event,
                tags: event.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Link)) && !link.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Link,
                tags: link.into_iter().collect(),
            });
        }
        if matches!(scope, None | Some(TagScope::Instrumentation)) && !instrumentation.is_empty() {
            out.push(ScopedTag {
                scope: TagScope::Instrumentation,
                tags: instrumentation.into_iter().collect(),
            });
        }
        Ok(out)
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        let tag = tag.strip_prefix('.').unwrap_or(tag);
        let (attr_tag, attr_scope) = scoped_attribute_tag(tag);
        let mut values = BTreeSet::new();
        for trace in self.traces_in_range(tenant, start_ns, end_ns) {
            collect_trace_intrinsic_values(trace, tag, &mut values);
            if matches!(attr_scope, None | Some(TagScope::Resource)) && attr_tag == "service.name" {
                values.insert(("string".to_string(), trace.root_service_name.clone()));
            }
            for (idx, input) in trace.spans.iter().enumerate() {
                collect_span_intrinsic_values(input, &trace.nested, idx, tag, &mut values);
                collect_event_values(input, tag, &mut values);
                collect_link_values(input, tag, &mut values);
                if matches!(
                    attr_scope,
                    None | Some(TagScope::Span | TagScope::Instrumentation)
                ) {
                    values.extend(
                        input
                            .attrs
                            .iter()
                            .filter(|(key, _)| {
                                key == attr_tag
                                    || (attr_scope == Some(TagScope::Instrumentation)
                                        && key
                                            .strip_prefix(INSTRUMENTATION_ATTR_PREFIX)
                                            .is_some_and(|key| key == attr_tag))
                            })
                            .map(|(_, value)| typed_value_parts(value)),
                    );
                }
            }
        }
        Ok(values
            .into_iter()
            .map(|(type_, value)| TypedValue { type_, value })
            .collect())
    }
}

impl InMemorySpanStore {
    pub(crate) fn traces_in_range(&self, tenant: &str, start_ns: i64, end_ns: i64) -> Vec<&StoredTrace> {
        self.traces
            .get(tenant)
            .into_iter()
            .flatten()
            .filter(|trace| {
                start_ns <= trace.trace_start_unix_nano && trace.trace_start_unix_nano <= end_ns
            })
            .collect()
    }
}
