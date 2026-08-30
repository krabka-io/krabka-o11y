use super::*;

pub(crate) fn block_attr_values_for_key(
    str_values: Option<&ListArray>,
    int_values: Option<&ListArray>,
    double_values: Option<&ListArray>,
    bool_values: Option<&ListArray>,
    row: usize,
    attr_idx: usize,
) -> Result<Vec<AttrValue>, TraceqlError> {
    let values = string_attr_values(str_values, row, attr_idx, SCOL_ATTR_VALUE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Str).collect());
    }
    let values = i64_attr_values(int_values, row, attr_idx, SCOL_ATTR_VALUE_INT)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Int).collect());
    }
    let values = f64_attr_values(double_values, row, attr_idx, SCOL_ATTR_VALUE_DOUBLE)?;
    if !values.is_empty() {
        return Ok(values.into_iter().map(AttrValue::Float).collect());
    }
    Ok(
        bool_attr_values(bool_values, row, attr_idx, SCOL_ATTR_VALUE_BOOL)?
            .into_iter()
            .map(AttrValue::Bool)
            .collect(),
    )
}
