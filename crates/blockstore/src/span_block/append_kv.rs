use super::{ListBuilder, StringBuilder, StructBuilder};

pub(crate) fn append_kv(sb: &mut StructBuilder, attrs: &[(String, String)]) {
    let keys = sb
        .field_builder::<ListBuilder<StringBuilder>>(2)
        .expect("attr key list builder");
    for (key, _) in attrs {
        keys.values().append_value(key);
    }
    keys.append(true);

    let values = sb
        .field_builder::<ListBuilder<ListBuilder<StringBuilder>>>(3)
        .expect("attr value list builder");
    for (_, value) in attrs {
        values.values().values().append_value(value);
        values.values().append(true);
    }
    values.append(true);
}
