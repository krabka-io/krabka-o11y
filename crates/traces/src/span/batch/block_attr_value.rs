use super::{AttrValue, BlockAttrValue};

pub(crate) fn block_attr_value(value: &AttrValue) -> BlockAttrValue {
    match value {
        AttrValue::Str(value) => BlockAttrValue::Str(vec![value.clone()]),
        AttrValue::Int(value) => BlockAttrValue::Int(vec![*value]),
        AttrValue::Double(value) => BlockAttrValue::Double(vec![*value]),
        AttrValue::Bool(value) => BlockAttrValue::Bool(vec![*value]),
        AttrValue::Unsupported(value) => BlockAttrValue::Unsupported(value.clone()),
        AttrValue::Array(values) if values.is_empty() => BlockAttrValue::Str(Vec::new()),
        AttrValue::Array(values)
            if values
                .iter()
                .all(|value| matches!(value, AttrValue::Str(_))) =>
        {
            BlockAttrValue::Str(
                values
                    .iter()
                    .map(|value| match value {
                        AttrValue::Str(value) => value.clone(),
                        _ => unreachable!(),
                    })
                    .collect(),
            )
        }
        AttrValue::Array(values)
            if values
                .iter()
                .all(|value| matches!(value, AttrValue::Int(_))) =>
        {
            BlockAttrValue::Int(
                values
                    .iter()
                    .map(|value| match value {
                        AttrValue::Int(value) => *value,
                        _ => unreachable!(),
                    })
                    .collect(),
            )
        }
        AttrValue::Array(values)
            if values
                .iter()
                .all(|value| matches!(value, AttrValue::Double(_))) =>
        {
            BlockAttrValue::Double(
                values
                    .iter()
                    .map(|value| match value {
                        AttrValue::Double(value) => *value,
                        _ => unreachable!(),
                    })
                    .collect(),
            )
        }
        AttrValue::Array(values)
            if values
                .iter()
                .all(|value| matches!(value, AttrValue::Bool(_))) =>
        {
            BlockAttrValue::Bool(
                values
                    .iter()
                    .map(|value| match value {
                        AttrValue::Bool(value) => *value,
                        _ => unreachable!(),
                    })
                    .collect(),
            )
        }
        AttrValue::Array(_) | AttrValue::Bytes(_) => {
            BlockAttrValue::Unsupported(value.otlp_json().to_string())
        }
    }
}
