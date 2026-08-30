use super::BlockAttrValue;

pub(crate) fn same_block_attr_type(lhs: &BlockAttrValue, rhs: &BlockAttrValue) -> bool {
    matches!(
        (lhs, rhs),
        (BlockAttrValue::Str(_), BlockAttrValue::Str(_))
            | (BlockAttrValue::Int(_), BlockAttrValue::Int(_))
            | (BlockAttrValue::Double(_), BlockAttrValue::Double(_))
            | (BlockAttrValue::Bool(_), BlockAttrValue::Bool(_))
    )
}
