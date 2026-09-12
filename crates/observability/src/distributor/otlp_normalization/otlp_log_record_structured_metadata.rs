use super::{
    DistributorError, Labels, OtlpLogRecord, insert_metadata_if_absent, otlp_attributes_to_labels,
    otlp_severity_number_to_string,
};

pub(crate) fn otlp_log_record_structured_metadata(
    log_record: &OtlpLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = otlp_attributes_to_labels(log_record.attributes.as_deref())?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        log_record
            .severity_number
            .as_ref()
            .map(otlp_severity_number_to_string)
            .transpose()?,
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        log_record
            .severity_text
            .as_ref()
            .filter(|severity_text| !severity_text.is_empty())
            .cloned(),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "trace_id",
        log_record
            .trace_id
            .as_ref()
            .filter(|id| !id.is_empty())
            .cloned(),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "span_id",
        log_record
            .span_id
            .as_ref()
            .filter(|id| !id.is_empty())
            .cloned(),
    )?;
    Ok(metadata)
}
