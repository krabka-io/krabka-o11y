use super::{DecodedSeries, IngestEnforcer, LimitError, Limits};

pub(crate) fn enforce_label_limits(limits: &Limits, series: &[DecodedSeries]) -> Result<(), LimitError> {
    for series in series {
        IngestEnforcer::check_labels(limits, &series.labels)?;
    }
    Ok(())
}
