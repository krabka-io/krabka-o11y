use super::{
    TemplateControlExpression, TemplatePart, TemplateRenderContext, render_template_parts,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateWith {
    pub(crate) expression: TemplateControlExpression,
    pub(crate) parts: Vec<TemplatePart>,
    pub(crate) else_parts: Vec<TemplatePart>,
}

impl TemplateWith {
    pub(crate) fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        let (value, context) = self.expression.evaluate(context);
        if !value.is_truthy() {
            return render_template_parts(&self.else_parts, &context);
        }
        let child_context = context.with_current_dot(value);
        render_template_parts(&self.parts, &child_context)
    }
}
