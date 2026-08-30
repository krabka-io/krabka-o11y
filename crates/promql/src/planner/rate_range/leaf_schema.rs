use super::{Arc, DataType, Field, Schema, TIME_COLUMN, VALUE_COLUMN};

pub(crate) fn leaf_schema(label_names: &[String]) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        // Nullable so an ABSENT label (NULL) stays distinct from a PRESENT-but-
        // empty-valued label (`""`); see `super::leaf::leaf_schema`.
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    fields.push(Field::new(TIME_COLUMN, DataType::Int64, false));
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, false));
    Arc::new(Schema::new(fields))
}
