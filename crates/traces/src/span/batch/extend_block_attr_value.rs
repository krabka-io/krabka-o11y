use super::BlockAttrValue;

pub(crate) fn extend_block_attr_value(existing: &mut BlockAttrValue, next: BlockAttrValue) {
    match (existing, next) {
        (BlockAttrValue::Str(existing), BlockAttrValue::Str(next)) => existing.extend(next),
        (BlockAttrValue::Int(existing), BlockAttrValue::Int(next)) => existing.extend(next),
        (BlockAttrValue::Double(existing), BlockAttrValue::Double(next)) => existing.extend(next),
        (BlockAttrValue::Bool(existing), BlockAttrValue::Bool(next)) => existing.extend(next),
        _ => unreachable!("same_block_attr_type guards extension"),
    }
}
