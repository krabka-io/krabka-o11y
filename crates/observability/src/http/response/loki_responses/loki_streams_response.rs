use super::{
    BTreeMap, Labels, LokiStreamEncoding, LokiStreamEntry, Value,
    loki_streams_response_with_warnings,
};

pub(crate) fn loki_streams_response(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
    encoding: LokiStreamEncoding,
) -> Value {
    loki_streams_response_with_warnings(streams, &[], encoding)
}
