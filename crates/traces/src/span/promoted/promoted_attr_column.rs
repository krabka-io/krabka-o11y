use super::{
    Arc, ArrayRef, BooleanArray, Float64Array, Int32Type, Int64Array, PromotedSpanAttr,
    PromotedSpanAttrType, RecordBatch, SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL,
    SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT, StringArray, StringDictionaryBuilder, TracesError,
    promoted_attr_slots, promoted_values,
};

/// Rebuild a promoted attribute's dedicated column from a batch that lacks it.
///
/// The write path writes a promoted attribute twice: once into its dedicated
/// column and once into the generic attribute lists that every block carries.
/// The generic copy is therefore a complete source for the dedicated one, so a
/// block written before the attribute was promoted still yields the same
/// column a block written after it would have.
///
/// Filling the column with nulls instead would read correctly today, because
/// the query path falls back to the generic lists per row, but it would leave
/// a column that says "absent" where the value is present. Anything that later
/// pushes a filter down onto the dedicated column would silently lose rows.
pub(crate) fn promoted_attr_column(
    batch: &RecordBatch,
    attr: &PromotedSpanAttr,
) -> Result<ArrayRef, TracesError> {
    let slots = promoted_attr_slots(batch, &attr.key)?;
    match attr.value_type {
        PromotedSpanAttrType::String => {
            let values = promoted_values::<StringArray, _>(
                batch,
                SCOL_ATTR_VALUE,
                &slots,
                |values, idx| values.value(idx).to_string(),
            )?;
            let mut builder = StringDictionaryBuilder::<Int32Type>::new();
            for value in values {
                builder.append_option(value);
            }
            Ok(Arc::new(builder.finish()))
        }
        PromotedSpanAttrType::Int => {
            let values = promoted_values::<Int64Array, _>(
                batch,
                SCOL_ATTR_VALUE_INT,
                &slots,
                Int64Array::value,
            )?;
            Ok(Arc::new(values.into_iter().collect::<Int64Array>()))
        }
        PromotedSpanAttrType::Double => {
            let values = promoted_values::<Float64Array, _>(
                batch,
                SCOL_ATTR_VALUE_DOUBLE,
                &slots,
                Float64Array::value,
            )?;
            Ok(Arc::new(values.into_iter().collect::<Float64Array>()))
        }
        PromotedSpanAttrType::Bool => {
            let values = promoted_values::<BooleanArray, _>(
                batch,
                SCOL_ATTR_VALUE_BOOL,
                &slots,
                BooleanArray::value,
            )?;
            Ok(Arc::new(values.into_iter().collect::<BooleanArray>()))
        }
    }
}
