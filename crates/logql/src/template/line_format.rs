use std::collections::BTreeSet;

use super::{
    Labels, ParseError, TemplateCommand, TemplateExpression, TemplatePart, TemplateRenderContext,
    TemplateValue, parse_template_parts, render_template_parts,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LineFormat {
    pub(crate) template: String,
    pub(crate) parts: Vec<TemplatePart>,
}

impl LineFormat {
    /// # Errors
    /// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
    pub fn new(template: impl Into<String>) -> Result<Self, ParseError> {
        let template = template.into();
        let parts = parse_template_parts(&template)?;
        Ok(Self { template, parts })
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
        let context = TemplateRenderContext::new(line, fields, None).with_json_variables(variables);
        render_template_parts(&self.parts, &context)
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
        let context = TemplateRenderContext::new(line, fields, None)
            .with_json_variables(variables)
            .with_json_queries(queries);
        render_template_parts(&self.parts, &context)
    }

    pub(crate) fn render_with_timestamp(
        &self,
        line: &str,
        fields: &Labels,
        timestamp_ns: Option<i64>,
    ) -> String {
        let context = TemplateRenderContext::new(line, fields, timestamp_ns);
        render_template_parts(&self.parts, &context)
    }
}

fn collect_query_calls_from_parts(parts: &[TemplatePart], queries: &mut BTreeSet<String>) {
    for part in parts {
        match part {
            TemplatePart::Literal(_) | TemplatePart::Comment => {}
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
    if let TemplateValue::Expression(expression) = value {
        collect_query_calls_from_expression(expression, queries);
    }
}
