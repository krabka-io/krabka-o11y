use super::*;

pub(crate) fn loki_streams_response(streams: BTreeMap<Labels, Vec<[String; 2]>>) -> Value {
    loki_streams_response_with_warnings(streams, &[])
}
