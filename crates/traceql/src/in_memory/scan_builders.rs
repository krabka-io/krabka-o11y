use super::*;

pub(crate) struct ScanBuilders {
    pub(crate) trace_id: FixedSizeBinaryBuilder,
    pub(crate) span_id: FixedSizeBinaryBuilder,
    pub(crate) parent_span_id: FixedSizeBinaryBuilder,
    pub(crate) ns_left: Int32Builder,
    pub(crate) ns_right: Int32Builder,
    pub(crate) parent_id: Int32Builder,
    pub(crate) child_count: Int32Builder,
    pub(crate) root_service: StringBuilder,
    pub(crate) root_span: StringBuilder,
    pub(crate) trace_start: Int64Builder,
    pub(crate) trace_duration: Int64Builder,
    pub(crate) name: StringBuilder,
    pub(crate) kind: Int32Builder,
    pub(crate) start: Int64Builder,
    pub(crate) duration: Int64Builder,
    pub(crate) status_code: Int32Builder,
    pub(crate) status_message: StringBuilder,
    pub(crate) instrumentation_name: StringBuilder,
    pub(crate) instrumentation_version: StringBuilder,
    pub(crate) event_name: StringBuilder,
    pub(crate) event_time_since_start: Int64Builder,
    pub(crate) link_trace_id: FixedSizeBinaryBuilder,
    pub(crate) link_span_id: FixedSizeBinaryBuilder,
}

impl ScanBuilders {
    pub(crate) fn new(row_count: usize) -> Self {
        Self {
            trace_id: FixedSizeBinaryBuilder::with_capacity(row_count, 16),
            span_id: FixedSizeBinaryBuilder::with_capacity(row_count, 8),
            parent_span_id: FixedSizeBinaryBuilder::with_capacity(row_count, 8),
            ns_left: Int32Builder::new(),
            ns_right: Int32Builder::new(),
            parent_id: Int32Builder::new(),
            child_count: Int32Builder::new(),
            root_service: StringBuilder::new(),
            root_span: StringBuilder::new(),
            trace_start: Int64Builder::new(),
            trace_duration: Int64Builder::new(),
            name: StringBuilder::new(),
            kind: Int32Builder::new(),
            start: Int64Builder::new(),
            duration: Int64Builder::new(),
            status_code: Int32Builder::new(),
            status_message: StringBuilder::new(),
            instrumentation_name: StringBuilder::new(),
            instrumentation_version: StringBuilder::new(),
            event_name: StringBuilder::new(),
            event_time_since_start: Int64Builder::new(),
            link_trace_id: FixedSizeBinaryBuilder::with_capacity(row_count, 16),
            link_span_id: FixedSizeBinaryBuilder::with_capacity(row_count, 8),
        }
    }

    pub(crate) fn append(
        &mut self,
        trace: &StoredTrace,
        span: &InputSpan,
        index: usize,
        event: Option<&EventRef>,
        link: Option<&LinkRef>,
        attr_builders: &mut [(String, AttrBuilder)],
    ) -> Result<()> {
        self.trace_id
            .append_value(span.trace_id)
            .map_err(|error| TraceqlError::Store(error.to_string()))?;
        self.span_id
            .append_value(span.span_id)
            .map_err(|error| TraceqlError::Store(error.to_string()))?;
        if let Some(parent) = span.parent_span_id {
            self.parent_span_id
                .append_value(parent)
                .map_err(|error| TraceqlError::Store(error.to_string()))?;
        } else {
            self.parent_span_id.append_null();
        }
        let nested = trace.nested[index];
        self.ns_left.append_value(nested.left);
        self.ns_right.append_value(nested.right);
        self.parent_id.append_value(nested.parent_id);
        self.child_count
            .append_value(child_count_for(&trace.nested, index));
        self.root_service.append_value(&trace.root_service_name);
        self.root_span.append_value(&trace.root_span_name);
        self.trace_start.append_value(trace.trace_start_unix_nano);
        self.trace_duration
            .append_value(trace.trace_duration.nanos_i64());
        self.name.append_value(&span.name);
        self.kind.append_value(span.kind);
        self.start.append_value(span.start_unix_nano);
        self.duration.append_value(span.duration.nanos_i64());
        self.status_code.append_value(span.status_code);
        self.status_message.append_value(&span.status_message);
        self.instrumentation_name
            .append_value(&span.instrumentation_name);
        self.instrumentation_version
            .append_value(&span.instrumentation_version);
        self.append_event(event);
        self.append_link(link)?;
        for (key, builder) in attr_builders {
            builder.append(nested_attr_value(key, span, event, link));
        }
        Ok(())
    }

    pub(crate) fn append_event(&mut self, event: Option<&EventRef>) {
        if let Some(event) = event {
            self.event_name.append_value(&event.name);
            self.event_time_since_start
                .append_value(event.time_since_start.nanos_i64());
        } else {
            self.event_name.append_null();
            self.event_time_since_start.append_null();
        }
    }

    pub(crate) fn append_link(&mut self, link: Option<&LinkRef>) -> Result<()> {
        if let Some(link) = link {
            self.link_trace_id
                .append_value(link.trace_id)
                .map_err(|error| TraceqlError::Store(error.to_string()))?;
            self.link_span_id
                .append_value(link.span_id)
                .map_err(|error| TraceqlError::Store(error.to_string()))?;
        } else {
            self.link_trace_id.append_null();
            self.link_span_id.append_null();
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.parent_span_id.finish()),
            Arc::new(self.ns_left.finish()),
            Arc::new(self.ns_right.finish()),
            Arc::new(self.parent_id.finish()),
            Arc::new(self.child_count.finish()),
            Arc::new(self.root_service.finish()),
            Arc::new(self.root_span.finish()),
            Arc::new(self.trace_start.finish()),
            Arc::new(self.trace_duration.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.kind.finish()),
            Arc::new(self.start.finish()),
            Arc::new(self.duration.finish()),
            Arc::new(self.status_code.finish()),
            Arc::new(self.status_message.finish()),
            Arc::new(self.instrumentation_name.finish()),
            Arc::new(self.instrumentation_version.finish()),
            Arc::new(self.event_name.finish()),
            Arc::new(self.event_time_since_start.finish()),
            Arc::new(self.link_trace_id.finish()),
            Arc::new(self.link_span_id.finish()),
        ]
    }
}
