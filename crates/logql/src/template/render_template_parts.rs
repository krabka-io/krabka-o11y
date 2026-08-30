use super::*;

pub(crate) fn render_template_parts(parts: &[TemplatePart], context: &TemplateRenderContext<'_>) -> String {
    let mut context = context.clone();
    let mut rendered = String::new();
    for part in parts {
        match part {
            TemplatePart::Literal(literal) => rendered.push_str(literal),
            TemplatePart::Comment => {}
            TemplatePart::Expression(expression) => {
                rendered.push_str(&expression.render(&context));
            }
            TemplatePart::Conditional(conditional) => {
                rendered.push_str(&conditional.render(&context));
            }
            TemplatePart::Range(range) => {
                rendered.push_str(&range.render(&context));
            }
            TemplatePart::With(with) => {
                rendered.push_str(&with.render(&context));
            }
            TemplatePart::Assignment(assignment) => {
                let value = assignment.expression.evaluate(&context);
                context = context.with_variable(assignment.variable.clone(), value);
            }
        }
    }
    rendered
}
