use super::{
    TemplateExpression, TemplatePart, TemplateRangeBinding, TemplateRenderContext,
    TemplateRuntimeValue,
};

pub(super) type Entries = Box<dyn Iterator<Item = (TemplateRuntimeValue, TemplateRuntimeValue)>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateRange {
    pub(crate) binding: TemplateRangeBinding,
    pub(crate) expression: TemplateExpression,
    pub(crate) parts: Vec<TemplatePart>,
    pub(crate) else_parts: Vec<TemplatePart>,
}

impl TemplateRange {
    pub(crate) fn entries(
        &self,
        context: &TemplateRenderContext<'_>,
        value: TemplateRuntimeValue,
    ) -> Entries {
        if context.error.borrow().is_some() {
            return Box::new(std::iter::empty());
        }
        match value {
            TemplateRuntimeValue::FloatSlice(values)
            | TemplateRuntimeValue::HistogramSpans(values) => {
                let mut index = 0;
                Box::new(std::iter::from_fn(move || {
                    let value = values.get(index)?;
                    let entry = (
                        TemplateRuntimeValue::Integer(
                            i64::try_from(index).expect("template collection index fits"),
                        ),
                        value,
                    );
                    index += 1;
                    Some(entry)
                }))
            }
            TemplateRuntimeValue::ByteSlice(values) => {
                Box::new(values.into_iter().enumerate().map(|(index, value)| {
                    (
                        TemplateRuntimeValue::Integer(
                            i64::try_from(index).expect("template collection index fits"),
                        ),
                        TemplateRuntimeValue::Byte(value),
                    )
                }))
            }
            TemplateRuntimeValue::QueryResult(values) => {
                Box::new((0..values.len()).map(move |index| {
                    (
                        TemplateRuntimeValue::Integer(
                            i64::try_from(index).expect("template collection index fits"),
                        ),
                        values.get(index).expect("queryResult range retains length"),
                    )
                }))
            }
            TemplateRuntimeValue::Array(values) => {
                Box::new(values.into_iter().enumerate().map(|(index, value)| {
                    (
                        TemplateRuntimeValue::Integer(
                            i64::try_from(index).expect("template collection index fits"),
                        ),
                        value,
                    )
                }))
            }
            TemplateRuntimeValue::Object(values) => Box::new(
                values
                    .into_iter()
                    .map(|(key, value)| (TemplateRuntimeValue::String(key), value)),
            ),
            TemplateRuntimeValue::ByteLabels(values) => {
                Box::new(values.into_iter().map(|(key, value)| {
                    (
                        TemplateRuntimeValue::String(key),
                        TemplateRuntimeValue::Bytes(value),
                    )
                }))
            }
            TemplateRuntimeValue::Labels(values) => {
                Box::new(values.into_iter().map(|(key, value)| {
                    (
                        TemplateRuntimeValue::String(key),
                        TemplateRuntimeValue::String(value),
                    )
                }))
            }
            TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
                Box::new(values.into_iter().enumerate().map(|(index, value)| {
                    (
                        TemplateRuntimeValue::Integer(
                            i64::try_from(index).expect("template collection index fits"),
                        ),
                        TemplateRuntimeValue::Json(value),
                    )
                }))
            }
            TemplateRuntimeValue::Json(serde_json::Value::Object(values)) => {
                let mut values = values.into_iter().collect::<Vec<_>>();
                values.sort_by(|(left, _), (right, _)| left.cmp(right));
                Box::new(values.into_iter().map(|(key, value)| {
                    (
                        TemplateRuntimeValue::String(key),
                        TemplateRuntimeValue::Json(value),
                    )
                }))
            }
            TemplateRuntimeValue::Integer(_)
            | TemplateRuntimeValue::Integer64(_)
            | TemplateRuntimeValue::Byte(_)
            | TemplateRuntimeValue::Month(_)
            | TemplateRuntimeValue::Weekday(_)
            | TemplateRuntimeValue::Duration(_) => {
                if matches!(
                    self.binding,
                    TemplateRangeBinding::IndexValue { .. }
                        | TemplateRangeBinding::AssignIndexValue { .. }
                ) {
                    *context.error.borrow_mut() =
                        Some("integer range cannot use two variables".into());
                    return Box::new(std::iter::empty());
                }
                match value {
                    TemplateRuntimeValue::Integer(count) => Box::new((0..count).map(|index| {
                        (
                            TemplateRuntimeValue::Json(serde_json::Value::Null),
                            TemplateRuntimeValue::Integer(index),
                        )
                    })),
                    TemplateRuntimeValue::Integer64(count) => Box::new((0..count).map(|index| {
                        (
                            TemplateRuntimeValue::Json(serde_json::Value::Null),
                            TemplateRuntimeValue::Integer64(index),
                        )
                    })),
                    TemplateRuntimeValue::Byte(count) => Box::new((0..count).map(|index| {
                        (
                            TemplateRuntimeValue::Json(serde_json::Value::Null),
                            TemplateRuntimeValue::Byte(index),
                        )
                    })),
                    TemplateRuntimeValue::Duration(count) => Box::new((0..count).map(|index| {
                        (
                            TemplateRuntimeValue::Json(serde_json::Value::Null),
                            TemplateRuntimeValue::Duration(index),
                        )
                    })),
                    TemplateRuntimeValue::Month(count) => Box::new((0..count).map(|index| {
                        (
                            TemplateRuntimeValue::Json(serde_json::Value::Null),
                            TemplateRuntimeValue::Month(index),
                        )
                    })),
                    TemplateRuntimeValue::Weekday(count) => Box::new((0..count).map(|index| {
                        (
                            TemplateRuntimeValue::Json(serde_json::Value::Null),
                            TemplateRuntimeValue::Weekday(index),
                        )
                    })),
                    _ => unreachable!(),
                }
            }
            TemplateRuntimeValue::Json(serde_json::Value::Null) => Box::new(std::iter::empty()),
            _ => {
                *context.error.borrow_mut() = Some("range cannot iterate over this value".into());
                Box::new(std::iter::empty())
            }
        }
    }

    pub(crate) fn context<'a>(
        &self,
        context: &TemplateRenderContext<'a>,
        value: TemplateRuntimeValue,
    ) -> TemplateRenderContext<'a> {
        match &self.binding {
            TemplateRangeBinding::Dot => context.clone(),
            TemplateRangeBinding::Value(variable) => context.with_variable(variable.clone(), value),
            TemplateRangeBinding::IndexValue {
                index,
                value: variable,
            } => context
                .with_variable(index.clone(), value.clone())
                .with_variable(variable.clone(), value),
            TemplateRangeBinding::AssignValue(variable) => {
                context.assign_variable(variable, value);
                context.clone()
            }
            TemplateRangeBinding::AssignIndexValue {
                index,
                value: variable,
            } => {
                context.assign_variable(index, value.clone());
                context.assign_variable(variable, value);
                context.clone()
            }
        }
    }

    pub(crate) fn iteration<'a>(
        &self,
        context: &TemplateRenderContext<'a>,
        key: TemplateRuntimeValue,
        value: TemplateRuntimeValue,
    ) -> TemplateRenderContext<'a> {
        match &self.binding {
            TemplateRangeBinding::Dot => {}
            TemplateRangeBinding::Value(variable) | TemplateRangeBinding::AssignValue(variable) => {
                context.assign_variable(variable, value.clone());
            }
            TemplateRangeBinding::IndexValue {
                index,
                value: variable,
            }
            | TemplateRangeBinding::AssignIndexValue {
                index,
                value: variable,
            } => {
                context.assign_variable(index, key);
                context.assign_variable(variable, value.clone());
            }
        }
        context.with_current_dot(value)
    }
}
