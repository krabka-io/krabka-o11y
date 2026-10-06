use std::{
    cell::{Cell, RefCell},
    rc::Rc,
};

use super::{BTreeMap, Labels, TemplatePart, TemplateRuntimeValue};

#[derive(Clone, Debug)]
pub(crate) struct TemplateRenderContext<'a> {
    pub(crate) templates: Option<&'a BTreeMap<String, Vec<TemplatePart>>>,
    pub(crate) depth: usize,
    pub(crate) prometheus_timestamp_ms: Option<i64>,
    pub(crate) discover_queries: bool,
    pub(crate) external_url: String,
    pub(crate) query_index: Rc<Cell<usize>>,
    pub(crate) query_results: Vec<TemplateRuntimeValue>,
    pub(crate) pending_query: Rc<RefCell<Option<String>>>,
    pub(crate) root_dot: Option<TemplateRuntimeValue>,
    pub(crate) flow: Rc<Cell<u8>>,
    pub(crate) error: Rc<RefCell<Option<String>>>,
    pub(crate) line: &'a str,
    pub(crate) fields: &'a Labels,
    pub(crate) timestamp_ns: Option<i64>,
    pub(crate) variables: BTreeMap<String, Rc<RefCell<TemplateRuntimeValue>>>,
    pub(crate) queries: BTreeMap<String, TemplateRuntimeValue>,
    pub(crate) current_dot: Option<TemplateRuntimeValue>,
}

impl<'a> TemplateRenderContext<'a> {
    pub(crate) fn new(line: &'a str, fields: &'a Labels, timestamp_ns: Option<i64>) -> Self {
        Self {
            templates: None,
            depth: 0,
            prometheus_timestamp_ms: None,
            discover_queries: false,
            external_url: String::new(),
            query_index: Rc::new(Cell::new(0)),
            query_results: Vec::new(),
            pending_query: Rc::new(RefCell::new(None)),
            root_dot: None,
            flow: Rc::new(Cell::new(0)),
            error: Rc::new(RefCell::new(None)),
            line,
            fields,
            timestamp_ns,
            variables: BTreeMap::new(),
            queries: BTreeMap::new(),
            current_dot: Some(super::template_root_field_value(fields, &[])),
        }
    }

    pub(crate) fn with_templates(
        mut self,
        templates: &'a BTreeMap<String, Vec<TemplatePart>>,
    ) -> Self {
        self.templates = Some(templates);
        self
    }

    pub(crate) fn for_invocation(&self, value: TemplateRuntimeValue) -> Self {
        let mut context = self.with_current_dot(value.clone());
        context.root_dot = Some(value);
        context.variables.clear();
        context.depth += 1;
        context
    }

    pub(crate) fn with_variable(&self, name: String, value: TemplateRuntimeValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name, Rc::new(RefCell::new(value)));
        Self {
            templates: self.templates,
            depth: self.depth,
            prometheus_timestamp_ms: self.prometheus_timestamp_ms,
            discover_queries: self.discover_queries,
            external_url: self.external_url.clone(),
            query_index: Rc::clone(&self.query_index),
            query_results: self.query_results.clone(),
            pending_query: Rc::clone(&self.pending_query),
            root_dot: self.root_dot.clone(),
            flow: Rc::clone(&self.flow),
            error: Rc::clone(&self.error),
            line: self.line,
            fields: self.fields,
            timestamp_ns: self.timestamp_ns,
            variables,
            queries: self.queries.clone(),
            current_dot: self.current_dot.clone(),
        }
    }

    pub(crate) fn assign_variable(&self, name: &str, value: TemplateRuntimeValue) {
        if let Some(variable) = self.variables.get(name) {
            *variable.borrow_mut() = value;
        } else {
            *self.error.borrow_mut() = Some(format!("undefined variable: ${name}"));
        }
    }

    pub(crate) fn with_json_variables(
        mut self,
        variables: &BTreeMap<String, serde_json::Value>,
    ) -> Self {
        variables
            .get("externalURL")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .clone_into(&mut self.external_url);
        self.variables.extend(variables.iter().map(|(name, value)| {
            (
                name.clone(),
                Rc::new(RefCell::new(TemplateRuntimeValue::Json(value.clone()))),
            )
        }));
        self
    }

    pub(crate) fn with_json_queries(
        mut self,
        queries: &BTreeMap<String, serde_json::Value>,
    ) -> Self {
        self.queries = queries
            .iter()
            .map(|(key, value)| (key.clone(), TemplateRuntimeValue::Json(value.clone())))
            .collect();
        self
    }

    pub(crate) fn with_current_dot(&self, value: TemplateRuntimeValue) -> Self {
        Self {
            templates: self.templates,
            depth: self.depth,
            prometheus_timestamp_ms: self.prometheus_timestamp_ms,
            discover_queries: self.discover_queries,
            external_url: self.external_url.clone(),
            query_index: Rc::clone(&self.query_index),
            query_results: self.query_results.clone(),
            pending_query: Rc::clone(&self.pending_query),
            root_dot: self.root_dot.clone(),
            flow: Rc::clone(&self.flow),
            error: Rc::clone(&self.error),
            line: self.line,
            fields: self.fields,
            timestamp_ns: self.timestamp_ns,
            variables: self.variables.clone(),
            queries: self.queries.clone(),
            current_dot: Some(value),
        }
    }
}
