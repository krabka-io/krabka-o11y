use super::{AnyValue, AttrValue};

pub(crate) fn any_to_attr(value: &AnyValue) -> AttrValue {
    AttrValue::from(value)
}
