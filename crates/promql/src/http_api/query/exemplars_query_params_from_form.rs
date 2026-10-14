use super::{ApiError, ExemplarsQueryParams, query_range_form_fields};

pub(crate) fn exemplars_query_params_from_form(
    body: &[u8],
) -> Result<ExemplarsQueryParams, ApiError> {
    let fields = query_range_form_fields(body, |_, _| Ok(()))?;
    Ok(ExemplarsQueryParams {
        query: fields.query,
        start: fields.start,
        end: fields.end,
    })
}
