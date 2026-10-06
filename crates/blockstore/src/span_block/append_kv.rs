use super::{AttrValue, ListBuilder, SpanAttr, StringBuilder, StructBuilder};

/// Indexed text projections and the lossless typed values share one logical source.
pub(crate) fn append_kv(sb: &mut StructBuilder, attrs: &[SpanAttr]) {
    let keys = sb
        .field_builder::<ListBuilder<StringBuilder>>(2)
        .expect("attr key list builder");
    for attr in attrs {
        keys.values().append_value(&attr.key);
    }
    keys.append(true);
    let values = sb
        .field_builder::<ListBuilder<ListBuilder<StringBuilder>>>(3)
        .expect("attr value list builder");
    for attr in attrs {
        let projected = match &attr.value {
            AttrValue::Unsupported(_) => Vec::new(),
            AttrValue::Str(values) => values.clone(),
            AttrValue::Int(values) => values.iter().map(ToString::to_string).collect(),
            AttrValue::Double(values) => values.iter().map(ToString::to_string).collect(),
            AttrValue::Bool(values) => values.iter().map(ToString::to_string).collect(),
        };
        for value in projected {
            values.values().values().append_value(value);
        }
        values.values().append(true);
    }
    values.append(true);
    sb.field_builder::<StringBuilder>(4)
        .expect("typed attribute payload builder")
        .append_value(
            serde_json::to_string(attrs).expect("typed attributes have total JSON serialization"),
        );
}
