use super::BlockAttrValue;

pub(crate) fn extend_block_attr_value(existing: &mut BlockAttrValue, next: BlockAttrValue) {
    match existing {
        BlockAttrValue::Str(existing) => match next {
            BlockAttrValue::Str(next) => existing.extend(next),
            _ => unreachable!("same_block_attr_type guards extension"),
        },
        BlockAttrValue::Int(existing) => match next {
            BlockAttrValue::Int(next) => existing.extend(next),
            _ => unreachable!("same_block_attr_type guards extension"),
        },
        BlockAttrValue::Double(existing) => match next {
            BlockAttrValue::Double(next) => existing.extend(next),
            _ => unreachable!("same_block_attr_type guards extension"),
        },
        BlockAttrValue::Bool(existing) => match next {
            BlockAttrValue::Bool(next) => existing.extend(next),
            _ => unreachable!("same_block_attr_type guards extension"),
        },
        BlockAttrValue::Unsupported(_) => {
            unreachable!("same_block_attr_type guards extension")
        }
    }
}
