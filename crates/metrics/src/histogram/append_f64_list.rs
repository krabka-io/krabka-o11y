use super::*;

pub(crate) fn append_f64_list(builder: &mut ListBuilder<Float64Builder>, values: &[f64]) {
    for value in values {
        builder.values().append_value(*value);
    }
    builder.append(true);
}
