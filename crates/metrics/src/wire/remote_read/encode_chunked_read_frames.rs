use prost::Message;

use super::{RemoteReadError, encode_xor_chunks, v1};

pub fn encode_chunked_read_frames(
    response: v1::ReadResponse,
) -> impl Iterator<Item = Result<Vec<u8>, RemoteReadError>> {
    response
        .results
        .into_iter()
        .enumerate()
        .flat_map(|(query_index, result)| {
            result
                .timeseries
                .into_iter()
                .filter(|series| !series.samples.is_empty())
                .map(move |series| {
                    let query_index = i64::try_from(query_index)
                        .map_err(|_| RemoteReadError::TooManyQueries(query_index))?;
                    let response = v1::ChunkedReadResponse {
                        chunked_series: vec![v1::ChunkedSeries {
                            labels: series.labels,
                            chunks: encode_xor_chunks(&series.samples)?,
                        }],
                        query_index,
                    };
                    Ok(frame(response.encode_to_vec()))
                })
        })
}

fn frame(payload: Vec<u8>) -> Vec<u8> {
    let mut framed = Vec::with_capacity(payload.len() + 14);
    push_uvarint(&mut framed, payload.len() as u64);
    framed.extend_from_slice(&crc32c::crc32c(&payload).to_be_bytes());
    framed.extend(payload);
    framed
}

fn push_uvarint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).expect("masked uvarint byte fits in u8") | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).expect("terminal uvarint byte fits in u8"));
}
