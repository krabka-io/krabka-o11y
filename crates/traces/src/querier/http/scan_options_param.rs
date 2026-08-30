use super::{ScanJob, ScanOptions, Uri, optional_usize_param, query_param};

pub(crate) fn scan_options_param(uri: &Uri) -> Result<ScanOptions, String> {
    let block = query_param(uri, "block");
    let row_group_start = optional_usize_param(uri, "rowGroupStart")?;
    let row_group_end = optional_usize_param(uri, "rowGroupEnd")?;
    if block.is_none() && row_group_start.is_none() && row_group_end.is_none() {
        return Ok(ScanOptions::default());
    }
    let Some(object_key) = block else {
        return Err("missing query parameter block".into());
    };
    let Some(row_group_start) = row_group_start else {
        return Err("missing query parameter rowGroupStart".into());
    };
    let Some(row_group_end) = row_group_end else {
        return Err("missing query parameter rowGroupEnd".into());
    };
    if row_group_end <= row_group_start {
        return Err("rowGroupEnd must be > rowGroupStart".into());
    }
    Ok(ScanOptions {
        job: Some(ScanJob {
            object_key,
            row_group_start,
            row_group_end,
        }),
        ..ScanOptions::default()
    })
}
