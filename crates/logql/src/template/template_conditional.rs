use super::{
    TemplateControlExpression, TemplatePart, TemplateRenderContext, render_template_parts,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateConditional {
    pub(crate) branches: Vec<(TemplateControlExpression, Vec<TemplatePart>)>,
    pub(crate) else_parts: Vec<TemplatePart>,
}

impl TemplateConditional {
    pub(crate) fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        let mut context = context.clone();
        for (condition, parts) in &self.branches {
            let (value, branch_context) = condition.evaluate(&context);
            if value.is_truthy() {
                return render_template_parts(parts, &branch_context);
            }
            context = branch_context;
        }
        render_template_parts(&self.else_parts, &context)
    }
}
