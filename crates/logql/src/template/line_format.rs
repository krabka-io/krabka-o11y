use super::{TemplatePart, ParseError, parse_template_parts, Labels, TemplateRenderContext, render_template_parts};

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

    #[must_use]
    pub fn render(&self, line: &str, fields: &Labels) -> String {
        self.render_with_timestamp(line, fields, None)
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
