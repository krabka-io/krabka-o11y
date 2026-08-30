use super::*;

pub(crate) fn append_attrs(
    attrs: &[SpanAttr],
    keys: &mut ListBuilder<StringBuilder>,
    is_array: &mut ListBuilder<BooleanBuilder>,
    str_values: &mut ListBuilder<ListBuilder<StringBuilder>>,
    int_values: &mut ListBuilder<ListBuilder<Int64Builder>>,
    double_values: &mut ListBuilder<ListBuilder<Float64Builder>>,
    bool_values: &mut ListBuilder<ListBuilder<BooleanBuilder>>,
) {
    for attr in attrs {
        keys.values().append_value(&attr.key);
        is_array.values().append_value(attr.is_array);

        match &attr.value {
            AttrValue::Str(values) => {
                for value in values {
                    str_values.values().values().append_value(value);
                }
            }
            AttrValue::Int(values) => {
                for &value in values {
                    int_values.values().values().append_value(value);
                }
            }
            AttrValue::Double(values) => {
                for &value in values {
                    double_values.values().values().append_value(value);
                }
            }
            AttrValue::Bool(values) => {
                for &value in values {
                    bool_values.values().values().append_value(value);
                }
            }
        }

        str_values.values().append(true);
        int_values.values().append(true);
        double_values.values().append(true);
        bool_values.values().append(true);
    }
    keys.append(true);
    is_array.append(true);
    str_values.append(true);
    int_values.append(true);
    double_values.append(true);
    bool_values.append(true);
}
