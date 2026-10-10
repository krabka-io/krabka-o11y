use krabka_blockstore::SpanColumnBuilders;

use super::*;

pub(crate) struct ScanBuilders {
    pub(crate) span: SpanColumnBuilders,
    pub(crate) event_name: StringBuilder,
    pub(crate) event_time_since_start: Int64Builder,
    pub(crate) link_trace_id: FixedSizeBinaryBuilder,
    pub(crate) link_span_id: FixedSizeBinaryBuilder,
}

impl ScanBuilders {
    pub(crate) fn new(row_count: usize) -> Self {
        Self {
            span: SpanColumnBuilders::with_capacity(row_count),
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
        let columns = &mut self.span;
        columns
            .trace_id
            .append_value(span.trace_id)
            .map_err(|error| TraceqlError::Store(error.to_string()))?;
        columns
            .span_id
            .append_value(span.span_id)
            .map_err(|error| TraceqlError::Store(error.to_string()))?;
        if let Some(parent) = span.parent_span_id {
            columns
                .parent_span_id
                .append_value(parent)
                .map_err(|error| TraceqlError::Store(error.to_string()))?;
        } else {
            columns.parent_span_id.append_null();
        }
        let nested = trace.nested[index];
        columns.ns_left.append_value(nested.left);
        columns.ns_right.append_value(nested.right);
        columns.parent_id.append_value(nested.parent_id);
        columns
            .child_count
            .append_value(child_count_for(&trace.nested, index));
        columns.root_svc.append_value(&trace.root_service_name);
        columns.root_name.append_value(&trace.root_span_name);
        columns
            .trace_start
            .append_value(trace.trace_start_unix_nano);
        columns
            .trace_dur
            .append_value(trace.trace_duration.nanos_i64());
        columns.name.append_value(&span.name);
        columns.kind.append_value(span.kind);
        columns.start.append_value(span.start_unix_nano);
        columns.dur.append_value(span.duration.nanos_i64());
        columns.status.append_value(span.status_code);
        columns.status_msg.append_value(&span.status_message);
        columns
            .instrumentation_name
            .append_value(&span.instrumentation_name);
        columns
            .instrumentation_version
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
        let mut columns = self.span.finish();
        columns.extend([
            Arc::new(self.event_name.finish()) as ArrayRef,
            Arc::new(self.event_time_since_start.finish()),
            Arc::new(self.link_trace_id.finish()),
            Arc::new(self.link_span_id.finish()),
        ]);
        columns
    }
}
