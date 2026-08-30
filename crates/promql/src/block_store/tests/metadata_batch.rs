use super::*;

pub(crate) fn metadata_batch(
    fingerprint: u64,
    metric_family_name: &str,
    metric_type: &str,
    help: &str,
    unit: &str,
) -> RecordBatch {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut names = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut helps = StringBuilder::new();
    let mut units = StringBuilder::new();

    fingerprints.append_value(fingerprint);
    timestamps.append_value(0);
    names.append_value(metric_family_name);
    types.append_value(metric_type);
    helps.append_value(help);
    units.append_value(unit);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(names.finish()),
        Arc::new(types.finish()),
        Arc::new(helps.finish()),
        Arc::new(units.finish()),
    ];
    RecordBatch::try_new(metadata_schema(), columns).unwrap()
}
