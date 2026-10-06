use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
};

use super::{AttrValue, InputSpan};

pub(crate) fn raw_attribute_columns(spans: &[&InputSpan]) -> Vec<(&'static str, ArrayRef)> {
    let mut keys = ListBuilder::new(StringBuilder::new());
    let mut strings = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
    let mut ints = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
    let mut floats = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
    let mut bools = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
    for span in spans {
        for (key, value) in &span.attrs {
            keys.values().append_value(key);
            if let AttrValue::Str(value) = value {
                strings.values().values().append_value(value);
            }
            if let AttrValue::Int(value) = value {
                ints.values().values().append_value(*value);
            }
            if let AttrValue::Float(value) = value {
                floats.values().values().append_value(*value);
            }
            if let AttrValue::Bool(value) = value {
                bools.values().values().append_value(*value);
            }
            strings.values().append(true);
            ints.values().append(true);
            floats.values().append(true);
            bools.values().append(true);
        }
        keys.append(true);
        strings.append(true);
        ints.append(true);
        floats.append(true);
        bools.append(true);
    }
    vec![
        ("attr_keys", Arc::new(keys.finish())),
        ("attr_value", Arc::new(strings.finish())),
        ("attr_value_int", Arc::new(ints.finish())),
        ("attr_value_double", Arc::new(floats.finish())),
        ("attr_value_bool", Arc::new(bools.finish())),
    ]
}
