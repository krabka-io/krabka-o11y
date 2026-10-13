use super::{HttpQueryError, MapArray, MapBuilder, StringBuilder};

pub(crate) fn loki_parquet_label_array(
    label_sets: &[Vec<krabka_blockstore::LabelPair>],
) -> Result<MapArray, HttpQueryError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for labels in label_sets {
        for label in labels {
            builder.keys().append_value(&label.name);
            builder.values().append_value(&label.value);
        }
        builder.append(true)?;
    }
    Ok(builder.finish())
}
