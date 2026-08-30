use super::{Array, BucketSpan, Int32Builder, ListBuilder, StructBuilder, UInt32Builder};

pub(crate) fn append_spans(builder: &mut ListBuilder<StructBuilder>, spans: &[BucketSpan]) {
    let struct_builder = builder.values();
    for span in spans {
        struct_builder
            .field_builder::<Int32Builder>(0)
            .expect("span offset builder")
            .append_value(span.offset);
        struct_builder
            .field_builder::<UInt32Builder>(1)
            .expect("span length builder")
            .append_value(span.length);
        struct_builder.append(true);
    }
    builder.append(true);
}
