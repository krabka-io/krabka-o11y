use super::*;

pub(crate) struct SpanColumnBuilders {
    pub(crate) trace_id: FixedSizeBinaryBuilder,
    pub(crate) span_id: FixedSizeBinaryBuilder,
    pub(crate) parent_span_id: FixedSizeBinaryBuilder,
    pub(crate) ns_left: Int32Builder,
    pub(crate) ns_right: Int32Builder,
    pub(crate) parent_id: Int32Builder,
    pub(crate) child_count: Int32Builder,
    pub(crate) root_svc: StringBuilder,
    pub(crate) root_name: StringBuilder,
    pub(crate) trace_start: Int64Builder,
    pub(crate) trace_dur: Int64Builder,
    pub(crate) name: StringBuilder,
    pub(crate) kind: Int32Builder,
    pub(crate) start: Int64Builder,
    pub(crate) dur: Int64Builder,
    pub(crate) status: Int32Builder,
    pub(crate) status_msg: StringBuilder,
    pub(crate) instrumentation_name: StringBuilder,
    pub(crate) instrumentation_version: StringBuilder,
}

impl SpanColumnBuilders {
    pub(crate) fn new() -> Self {
        Self {
            trace_id: FixedSizeBinaryBuilder::new(16),
            span_id: FixedSizeBinaryBuilder::new(8),
            parent_span_id: FixedSizeBinaryBuilder::new(8),
            ns_left: Int32Builder::new(),
            ns_right: Int32Builder::new(),
            parent_id: Int32Builder::new(),
            child_count: Int32Builder::new(),
            root_svc: StringBuilder::new(),
            root_name: StringBuilder::new(),
            trace_start: Int64Builder::new(),
            trace_dur: Int64Builder::new(),
            name: StringBuilder::new(),
            kind: Int32Builder::new(),
            start: Int64Builder::new(),
            dur: Int64Builder::new(),
            status: Int32Builder::new(),
            status_msg: StringBuilder::new(),
            instrumentation_name: StringBuilder::new(),
            instrumentation_version: StringBuilder::new(),
        }
    }

    pub(crate) fn append(&mut self, row: &SpanRow) -> Result<()> {
        self.trace_id
            .append_value(row.trace_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        self.span_id
            .append_value(row.span_id)
            .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?;
        match row.parent_span_id {
            Some(parent) => self
                .parent_span_id
                .append_value(parent)
                .map_err(|e| BlockStoreError::InvalidBlock(e.to_string()))?,
            None => self.parent_span_id.append_null(),
        }
        self.ns_left.append_value(row.nested_set.nested_set_left);
        self.ns_right.append_value(row.nested_set.nested_set_right);
        self.parent_id.append_value(row.nested_set.parent_id);
        self.child_count.append_value(row.child_count);
        self.root_svc
            .append_option(row.root_service_name.as_deref());
        self.root_name.append_option(row.root_span_name.as_deref());
        self.trace_start.append_value(row.trace_start_unix_nano);
        self.trace_dur.append_value(row.trace_duration.nanos_i64());
        self.name.append_option(row.name.as_deref());
        self.kind.append_value(row.kind.as_i32());
        self.start.append_value(row.start_unix_nano);
        self.dur.append_value(row.duration.nanos_i64());
        self.status.append_value(row.status_code.as_i32());
        self.status_msg.append_option(row.status_message.as_deref());
        self.instrumentation_name
            .append_option(row.instrumentation_name.as_deref());
        self.instrumentation_version
            .append_option(row.instrumentation_version.as_deref());
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
            Arc::new(self.root_svc.finish()),
            Arc::new(self.root_name.finish()),
            Arc::new(self.trace_start.finish()),
            Arc::new(self.trace_dur.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.kind.finish()),
            Arc::new(self.start.finish()),
            Arc::new(self.dur.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.status_msg.finish()),
            Arc::new(self.instrumentation_name.finish()),
            Arc::new(self.instrumentation_version.finish()),
        ]
    }
}
