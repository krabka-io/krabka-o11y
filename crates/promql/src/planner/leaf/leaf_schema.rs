use super::{
    Arc, DataType, Field, SAMPLE_TIME_COLUMN, SampleTimePresence, Schema, TIME_COLUMN, VALUE_COLUMN,
};

/// The leaf-batch schema: one nullable `Binary` column per label name, then the
/// timestamp and value columns, then the sample-time duplicate when
/// `sample_time` includes it.
pub(crate) fn leaf_schema(label_names: &[String], sample_time: SampleTimePresence) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 3);
    for name in label_names {
        // Label columns are nullable so an ABSENT label (NULL) is distinguishable
        // from a PRESENT-but-empty-valued label (`""`). The reconstruction
        // (`engine::labels_from_batch`) maps NULL -> absent and `""` ->
        // present-empty, preserving the byte-exact label set through the chain.
        fields.push(Field::new(name, DataType::Binary, true));
    }
    fields.push(Field::new(TIME_COLUMN, DataType::Int64, false));
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, false));
    if sample_time == SampleTimePresence::Included {
        fields.push(Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false));
    }
    Arc::new(Schema::new(fields))
}
