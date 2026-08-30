use super::{BTreeMap, SeriesFingerprint, pb, Labels, ApiError, remote_read_labels};

pub(crate) fn remote_read_series<'a>(
    by_fp: &'a mut BTreeMap<SeriesFingerprint, pb::v1::TimeSeries>,
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    fp: SeriesFingerprint,
) -> Result<&'a mut pb::v1::TimeSeries, ApiError> {
    let labels = labels_by_fp
        .get(&fp)
        .ok_or_else(|| ApiError::bad_data("remote_read series labels not found"))?;
    Ok(by_fp.entry(fp).or_insert_with(|| pb::v1::TimeSeries {
        labels: remote_read_labels(labels),
        samples: Vec::new(),
        exemplars: Vec::new(),
        histograms: Vec::new(),
    }))
}
