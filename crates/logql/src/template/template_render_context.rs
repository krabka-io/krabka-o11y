use super::{Labels, BTreeMap, TemplateRuntimeValue};

#[derive(Clone, Debug)]
pub(crate) struct TemplateRenderContext<'a> {
    pub(crate) line: &'a str,
    pub(crate) fields: &'a Labels,
    pub(crate) timestamp_ns: Option<i64>,
    pub(crate) variables: BTreeMap<String, TemplateRuntimeValue>,
    pub(crate) current_dot: Option<TemplateRuntimeValue>,
}

impl<'a> TemplateRenderContext<'a> {
    pub(crate) fn new(line: &'a str, fields: &'a Labels, timestamp_ns: Option<i64>) -> Self {
        Self {
            line,
            fields,
            timestamp_ns,
            variables: BTreeMap::new(),
            current_dot: None,
        }
    }

    pub(crate) fn with_variable(&self, name: String, value: TemplateRuntimeValue) -> Self {
        let mut variables = self.variables.clone();
        variables.insert(name, value);
        Self {
            line: self.line,
            fields: self.fields,
            timestamp_ns: self.timestamp_ns,
            variables,
            current_dot: self.current_dot.clone(),
        }
    }

    pub(crate) fn with_current_dot(&self, value: TemplateRuntimeValue) -> Self {
        Self {
            line: self.line,
            fields: self.fields,
            timestamp_ns: self.timestamp_ns,
            variables: self.variables.clone(),
            current_dot: Some(value),
        }
    }
}
