use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateRange {
    pub(crate) binding: TemplateRangeBinding,
    pub(crate) expression: TemplateExpression,
    pub(crate) parts: Vec<TemplatePart>,
    pub(crate) else_parts: Vec<TemplatePart>,
}

impl TemplateRange {
    pub(crate) fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        let value = self.expression.evaluate(context);
        match value {
            TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
                self.render_array(context, values)
            }
            TemplateRuntimeValue::Json(serde_json::Value::Object(object)) => {
                self.render_object(context, object)
            }
            _ => render_template_parts(&self.else_parts, context),
        }
    }

    pub(crate) fn render_array(
        &self,
        context: &TemplateRenderContext<'_>,
        values: Vec<serde_json::Value>,
    ) -> String {
        if values.is_empty() {
            return render_template_parts(&self.else_parts, context);
        }
        let mut rendered = String::new();
        for (index, value) in values.into_iter().enumerate() {
            let key = TemplateRuntimeValue::Integer(
                i64::try_from(index).expect("template collection index fits in i64"),
            );
            let value = TemplateRuntimeValue::Json(value);
            rendered.push_str(&self.render_iteration(context, key, value));
        }
        rendered
    }

    pub(crate) fn render_object(
        &self,
        context: &TemplateRenderContext<'_>,
        object: serde_json::Map<String, serde_json::Value>,
    ) -> String {
        if object.is_empty() {
            return render_template_parts(&self.else_parts, context);
        }
        let mut entries = object.into_iter().collect::<Vec<_>>();
        entries.sort_by(|(left, _), (right, _)| left.cmp(right));

        let mut rendered = String::new();
        for (key, value) in entries {
            let key = TemplateRuntimeValue::String(key);
            let value = TemplateRuntimeValue::Json(value);
            rendered.push_str(&self.render_iteration(context, key, value));
        }
        rendered
    }

    pub(crate) fn render_iteration(
        &self,
        context: &TemplateRenderContext<'_>,
        key: TemplateRuntimeValue,
        value: TemplateRuntimeValue,
    ) -> String {
        let child_context = match &self.binding {
            TemplateRangeBinding::Dot => context.with_current_dot(value),
            TemplateRangeBinding::Value(variable) => context.with_variable(variable.clone(), value),
            TemplateRangeBinding::IndexValue {
                index: index_variable,
                value: value_variable,
            } => context
                .with_variable(index_variable.clone(), key)
                .with_variable(value_variable.clone(), value),
        };
        render_template_parts(&self.parts, &child_context)
    }
}
