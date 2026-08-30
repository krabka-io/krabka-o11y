use super::*;

pub(crate) fn encode_write_request(rows: &[WireTimeSeries]) -> Result<Vec<u8>, String> {
    let request = WriteRequest {
        timeseries: rows
            .iter()
            .map(|row| TimeSeries {
                labels: labels_to_proto(&row.labels),
                samples: samples_to_proto(row),
                exemplars: row
                    .exemplars
                    .iter()
                    .map(|exemplar| RemoteWriteExemplar {
                        labels: labels_to_proto(&exemplar.labels),
                        value: exemplar.value,
                        timestamp: exemplar.timestamp_ms,
                    })
                    .collect(),
                histograms: histograms_to_proto(row),
            })
            .collect(),
    };

    let mut protobuf = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut protobuf)
        .map_err(|err| format!("remote_write pb encode: {err}"))?;
    snap::raw::Encoder::new()
        .compress_vec(&protobuf)
        .map_err(|err| format!("remote_write snappy encode: {err}"))
}
