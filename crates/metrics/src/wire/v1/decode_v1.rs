use super::{
    ByteSize, DecodedExemplar, DecodedSample, DecodedSeries, Message, WireError, labels_from_v1,
    metadata_series_from_v1, pb, snappy_block_decode, v1_histogram_to_native,
};

/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn decode_v1(body: &[u8], max_decompressed: ByteSize) -> Result<Vec<DecodedSeries>, WireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let req = pb::v1::WriteRequest::decode(raw.as_slice())
        .map_err(|error| WireError::ProtobufDecode(error.to_string()))?;

    let mut out = Vec::with_capacity(req.timeseries.len());
    for series in req.timeseries {
        let labels = labels_from_v1(&series.labels)?;
        let samples = series
            .samples
            .into_iter()
            .map(|sample| DecodedSample::new(sample.timestamp, sample.value))
            .collect();
        let histograms = series
            .histograms
            .iter()
            .map(|histogram| Ok((histogram.timestamp, v1_histogram_to_native(histogram)?)))
            .collect::<Result<Vec<_>, WireError>>()?;
        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| {
                Ok(DecodedExemplar {
                    labels: labels_from_v1(&exemplar.labels)?,
                    timestamp_ms: exemplar.timestamp,
                    value: exemplar.value,
                })
            })
            .collect::<Result<Vec<_>, WireError>>()?;

        out.push(DecodedSeries {
            labels,
            samples,
            histograms,
            exemplars,
            metadata: None,
        });
    }

    for metadata in req.metadata {
        out.push(metadata_series_from_v1(metadata));
    }

    Ok(out)
}
