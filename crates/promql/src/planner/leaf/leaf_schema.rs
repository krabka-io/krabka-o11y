use super::{Arc, Schema, Field, DataType, TIME_COLUMN, VALUE_COLUMN, SAMPLE_TIME_COLUMN};

pub(crate) fn leaf_schema(label_names: &[String]) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 3);
    for name in label_names {
        // Label columns are nullable so an ABSENT label (NULL) is distinguishable
        // from a PRESENT-but-empty-valued label (`""`). The reconstruction
        // (`engine::labels_from_batch`) maps NULL -> absent and `""` ->
        // present-empty, preserving the byte-exact label set through the chain.
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new(TIME_COLUMN, DataType::Int64, false));
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, false));
    fields.push(Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}
