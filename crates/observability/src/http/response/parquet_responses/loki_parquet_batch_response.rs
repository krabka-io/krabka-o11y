use super::{
    ArrowWriter, HttpQueryError, IntoResponse, LOKI_PARQUET_CONTENT_TYPE, RecordBatch, Response,
    StatusCode,
};

pub(crate) fn loki_parquet_batch_response(batch: &RecordBatch) -> Result<Response, HttpQueryError> {
    let mut body = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut body, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
    }
    Ok((
        StatusCode::OK,
        [("content-type", LOKI_PARQUET_CONTENT_TYPE)],
        body,
    )
        .into_response())
}
