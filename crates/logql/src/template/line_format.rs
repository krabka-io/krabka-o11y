use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

use super::{
    Labels, ParseError, TemplateCommand, TemplateExpression, TemplatePart, TemplateRenderContext,
    TemplateValue, parse_template_parts, render_template_parts,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineFormat {
    pub(crate) template: String,
    prometheus: bool,
    pub(crate) templates: BTreeMap<String, Vec<TemplatePart>>,
    pub(crate) parts: Vec<TemplatePart>,
}

impl LineFormat {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(template: impl Into<String>) -> Result<Self, ParseError> {
        Self::new_for_profile(template.into(), false)
    }

    /// Parses the function bindings registered by the pinned Prometheus expander.
    /// # Errors
    /// Returns an error for malformed templates or functions unavailable in this profile.
    pub fn new_prometheus(template: impl Into<String>) -> Result<Self, ParseError> {
        Self::new_for_profile(template.into(), true)
    }

    fn new_for_profile(template: String, prometheus: bool) -> Result<Self, ParseError> {
        let parts = parse_template_parts(&template)?;
        validate_template_flow(&parts, false)?;
        validate_template_function_profile(&parts, prometheus)?;
        let mut templates = BTreeMap::new();
        collect_template_definitions(&parts, &mut templates, true)?;
        Ok(Self {
            template,
            prometheus,
            templates,
            parts,
        })
    }

    #[must_use]
    pub fn template(&self) -> &str {
        &self.template
    }

    /// Returns the literal Prometheus queries referenced by template actions.
    #[must_use]
    pub fn query_calls(&self) -> BTreeSet<String> {
        let mut queries = BTreeSet::new();
        collect_query_calls_from_parts(&self.parts, &mut queries);
        queries
    }

    fn context<'a>(
        &'a self,
        line: &'a str,
        fields: &'a Labels,
        timestamp_ns: Option<i64>,
    ) -> TemplateRenderContext<'a> {
        let mut context =
            TemplateRenderContext::new(line, fields, timestamp_ns).with_templates(&self.templates);
        context.prometheus_timestamp_ms = self
            .prometheus
            .then_some(timestamp_ns.unwrap_or_default().div_euclid(1_000_000));
        context
    }

    #[must_use]
    pub fn render(&self, line: &str, fields: &Labels) -> String {
        self.render_with_timestamp(line, fields, None)
    }

    /// Renders with caller-provided Go-template variables such as `$labels`.
    #[must_use]
    pub fn render_with_variables(
        &self,
        line: &str,
        fields: &Labels,
        variables: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> String {
        let context = self
            .context(line, fields, None)
            .with_json_variables(variables);
        super::template_bytes_to_string(&render_template_parts(&self.parts, &context))
    }

    /// Renders with caller-resolved results for Prometheus's asynchronous
    /// `query` template function.
    #[must_use]
    pub fn render_with_variables_and_queries(
        &self,
        line: &str,
        fields: &Labels,
        variables: &std::collections::BTreeMap<String, serde_json::Value>,
        queries: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> String {
        let context = self
            .context(line, fields, None)
            .with_json_variables(variables)
            .with_json_queries(queries);
        super::template_bytes_to_string(&render_template_parts(&self.parts, &context))
    }

    /// Renders rule templates without decoding Go's byte-valued strings.
    /// # Errors
    /// Returns an execution error for invalid function arguments or operations.
    pub fn render_bytes_with_variables_and_queries(
        &self,
        variables: &BTreeMap<String, super::TemplateRuntimeValue>,
        queries: &BTreeMap<String, super::TemplateRuntimeValue>,
    ) -> Result<Vec<u8>, String> {
        self.render_typed_bytes(variables, queries, self.prometheus.then_some(0), None)
            .map_err(|error| error.to_string())
    }

    /// Renders a Prometheus rule at a fixed timestamp, stopping at its first
    /// executed query whose result the caller has not supplied.
    /// # Errors
    /// Returns the query to resolve or the upstream execution error.
    pub fn render_prometheus_bytes(
        &self,
        variables: &BTreeMap<String, super::TemplateRuntimeValue>,
        results: &[super::TemplateRuntimeValue],
        eval_time_ms: i64,
    ) -> Result<Vec<u8>, super::TemplateRenderError> {
        self.render_typed_bytes(
            variables,
            &BTreeMap::new(),
            Some(eval_time_ms),
            Some(results),
        )
    }

    fn render_typed_bytes(
        &self,
        variables: &BTreeMap<String, super::TemplateRuntimeValue>,
        queries: &BTreeMap<String, super::TemplateRuntimeValue>,
        eval_time_ms: Option<i64>,
        query_results: Option<&[super::TemplateRuntimeValue]>,
    ) -> Result<Vec<u8>, super::TemplateRenderError> {
        let fields = BTreeMap::new();
        let mut context =
            TemplateRenderContext::new("", &fields, None).with_templates(&self.templates);
        context.prometheus_timestamp_ms = eval_time_ms;
        context.discover_queries = query_results.is_some();
        context.query_results = query_results
            .unwrap_or_default()
            .iter()
            .map(super::TemplateRuntimeValue::clone_for_query_execution)
            .collect();
        context.external_url = variables
            .get("externalURL")
            .map_or_else(String::new, super::TemplateRuntimeValue::as_rendered_string);
        context
            .variables
            .extend(variables.iter().map(|(name, value)| {
                (
                    name.clone(),
                    Rc::new(RefCell::new(value.clone_for_query_execution())),
                )
            }));
        context.queries.clone_from(queries);
        let rendered = render_template_parts(&self.parts, &context);
        if let Some(query) = context.pending_query.borrow().clone() {
            return Err(super::TemplateRenderError::NeedsQuery(query));
        }
        let error = context.error.borrow().clone();
        error.map_or(Ok(rendered), |error| {
            Err(super::TemplateRenderError::Execution(error))
        })
    }

    pub(crate) fn render_checked_with_timestamp(
        &self,
        line: &str,
        fields: &Labels,
        timestamp_ns: Option<i64>,
    ) -> Result<String, String> {
        let context = self.context(line, fields, timestamp_ns);
        let rendered =
            super::template_bytes_to_string(&render_template_parts(&self.parts, &context));
        let error = context.error.borrow_mut().take();
        error.map_or(Ok(rendered), Err)
    }

    pub(crate) fn render_with_timestamp(
        &self,
        line: &str,
        fields: &Labels,
        timestamp_ns: Option<i64>,
    ) -> String {
        let context = self.context(line, fields, timestamp_ns);
        super::template_bytes_to_string(&render_template_parts(&self.parts, &context))
    }
}

fn collect_query_calls_from_parts(parts: &[TemplatePart], queries: &mut BTreeSet<String>) {
    for part in parts {
        match part {
            TemplatePart::Literal(_)
            | TemplatePart::Comment
            | TemplatePart::Break
            | TemplatePart::Continue => {}
            TemplatePart::Definition { parts, .. } => {
                collect_query_calls_from_parts(parts, queries);
            }
            TemplatePart::Invocation { argument, .. } => {
                if let Some(argument) = argument {
                    collect_query_calls_from_expression(argument, queries);
                }
            }
            TemplatePart::Expression(expression) => {
                collect_query_calls_from_expression(expression, queries);
            }
            TemplatePart::Conditional(conditional) => {
                for (condition, parts) in &conditional.branches {
                    collect_query_calls_from_expression(&condition.expression, queries);
                    collect_query_calls_from_parts(parts, queries);
                }
                collect_query_calls_from_parts(&conditional.else_parts, queries);
            }
            TemplatePart::Range(range) => {
                collect_query_calls_from_expression(&range.expression, queries);
                collect_query_calls_from_parts(&range.parts, queries);
                collect_query_calls_from_parts(&range.else_parts, queries);
            }
            TemplatePart::With(with) => {
                collect_query_calls_from_expression(&with.expression.expression, queries);
                collect_query_calls_from_parts(&with.parts, queries);
                collect_query_calls_from_parts(&with.else_parts, queries);
            }
            TemplatePart::Assignment(assignment) => {
                collect_query_calls_from_expression(&assignment.expression, queries);
            }
        }
    }
}

fn collect_query_calls_from_expression(
    expression: &TemplateExpression,
    queries: &mut BTreeSet<String>,
) {
    for command in &expression.commands {
        match command {
            TemplateCommand::Value(value) => collect_query_calls_from_value(value, queries),
            TemplateCommand::Call { callee, args } => {
                collect_query_calls_from_value(callee, queries);
                for arg in args {
                    collect_query_calls_from_value(arg, queries);
                }
            }
            TemplateCommand::Function { name, args } => {
                if name == "query"
                    && let Some(TemplateValue::String(query)) = args.first()
                {
                    queries.insert(query.clone());
                }
                for arg in args {
                    collect_query_calls_from_value(arg, queries);
                }
            }
        }
    }
}

fn collect_query_calls_from_value(value: &TemplateValue, queries: &mut BTreeSet<String>) {
    match value {
        TemplateValue::Expression(expression)
        | TemplateValue::ExpressionPath { expression, .. } => {
            collect_query_calls_from_expression(expression, queries);
        }
        _ => {}
    }
}

fn validate_template_flow(parts: &[TemplatePart], in_range: bool) -> Result<(), ParseError> {
    for part in parts {
        match part {
            TemplatePart::Break | TemplatePart::Continue if !in_range => {
                return Err(super::template_parse_error("range control outside range"));
            }
            TemplatePart::Definition { parts, .. } => validate_template_flow(parts, false)?,
            TemplatePart::Range(range) => {
                validate_template_flow(&range.parts, true)?;
                validate_template_flow(&range.else_parts, in_range)?;
            }
            TemplatePart::Conditional(conditional) => {
                for (_, parts) in &conditional.branches {
                    validate_template_flow(parts, in_range)?;
                }
                validate_template_flow(&conditional.else_parts, in_range)?;
            }
            TemplatePart::With(with) => {
                validate_template_flow(&with.parts, in_range)?;
                validate_template_flow(&with.else_parts, in_range)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn template_body_is_empty(parts: &[TemplatePart]) -> bool {
    parts.iter().all(|part| {
        matches!(part, TemplatePart::Comment)
            || matches!(part, TemplatePart::Literal(value) if value.trim().is_empty())
    })
}

fn collect_template_definitions(
    parts: &[TemplatePart],
    templates: &mut BTreeMap<String, Vec<TemplatePart>>,
    top_level: bool,
) -> Result<(), ParseError> {
    for part in parts {
        match part {
            TemplatePart::Definition { name, parts, block } => {
                if !top_level && !block {
                    return Err(super::template_parse_error(
                        "define must appear at top level",
                    ));
                }
                if let Some(previous) = templates.get(name) {
                    if template_body_is_empty(parts) {
                        continue;
                    }
                    if !template_body_is_empty(previous) {
                        return Err(super::template_parse_error("duplicate template definition"));
                    }
                }
                templates.insert(name.clone(), parts.clone());
                collect_template_definitions(parts, templates, false)?;
            }
            TemplatePart::Range(range) => {
                collect_template_definitions(&range.parts, templates, false)?;
                collect_template_definitions(&range.else_parts, templates, false)?;
            }
            TemplatePart::With(with) => {
                collect_template_definitions(&with.parts, templates, false)?;
                collect_template_definitions(&with.else_parts, templates, false)?;
            }
            TemplatePart::Conditional(conditional) => {
                for (_, parts) in &conditional.branches {
                    collect_template_definitions(parts, templates, false)?;
                }
                collect_template_definitions(&conditional.else_parts, templates, false)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_template_function_profile(
    parts: &[TemplatePart],
    prometheus: bool,
) -> Result<(), ParseError> {
    for part in parts {
        match part {
            TemplatePart::Expression(expression) => {
                validate_function_expression(expression, prometheus)?;
            }
            TemplatePart::Assignment(assignment) => {
                validate_function_expression(&assignment.expression, prometheus)?;
            }
            TemplatePart::Invocation {
                argument: Some(expression),
                ..
            } => validate_function_expression(expression, prometheus)?,
            TemplatePart::Conditional(conditional) => {
                for (condition, parts) in &conditional.branches {
                    validate_function_expression(&condition.expression, prometheus)?;
                    validate_template_function_profile(parts, prometheus)?;
                }
                validate_template_function_profile(&conditional.else_parts, prometheus)?;
            }
            TemplatePart::With(with) => {
                validate_function_expression(&with.expression.expression, prometheus)?;
                validate_template_function_profile(&with.parts, prometheus)?;
                validate_template_function_profile(&with.else_parts, prometheus)?;
            }
            TemplatePart::Range(range) => {
                validate_function_expression(&range.expression, prometheus)?;
                validate_template_function_profile(&range.parts, prometheus)?;
                validate_template_function_profile(&range.else_parts, prometheus)?;
            }
            TemplatePart::Definition { parts, .. } => {
                validate_template_function_profile(parts, prometheus)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_function_expression(
    expression: &TemplateExpression,
    prometheus: bool,
) -> Result<(), ParseError> {
    for command in &expression.commands {
        match command {
            TemplateCommand::Function { name, args } => {
                validate_function_name(name, prometheus)?;
                for value in args {
                    validate_function_value(value, prometheus)?;
                }
            }
            TemplateCommand::Call { callee, args } => {
                validate_function_value(callee, prometheus)?;
                for value in args {
                    validate_function_value(value, prometheus)?;
                }
            }
            TemplateCommand::Value(value) => validate_function_value(value, prometheus)?,
        }
    }
    Ok(())
}

fn validate_function_value(value: &TemplateValue, prometheus: bool) -> Result<(), ParseError> {
    match value {
        TemplateValue::Expression(expression)
        | TemplateValue::ExpressionPath { expression, .. } => {
            validate_function_expression(expression, prometheus)
        }
        TemplateValue::Function(name) => validate_function_name(name, prometheus),
        _ => Ok(()),
    }
}

fn validate_function_name(name: &str, prometheus: bool) -> Result<(), ParseError> {
    let supported = if prometheus {
        super::prometheus_functions::is_name(name) || super::prometheus_functions::is_builtin(name)
    } else {
        super::prometheus_functions::is_loki_name(name)
            || super::prometheus_functions::is_builtin(name)
    };
    if supported {
        Ok(())
    } else {
        Err(super::template_parse_error(
            "function is not registered in this template profile",
        ))
    }
}
