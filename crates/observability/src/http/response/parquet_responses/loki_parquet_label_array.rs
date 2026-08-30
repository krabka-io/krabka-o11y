use super::*;

pub(crate) fn loki_parquet_label_array(
    label_sets: &[Vec<(String, String)>],
) -> Result<MapArray, HttpQueryError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for labels in label_sets {
        for (key, value) in labels {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true)?;
    }
    Ok(builder.finish())
}
