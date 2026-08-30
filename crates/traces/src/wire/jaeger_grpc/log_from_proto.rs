use super::{JaegerLog, api_v2, key_value_from_proto, timestamp_micros};

pub(crate) fn log_from_proto(log: &api_v2::Log) -> JaegerLog {
    JaegerLog {
        timestamp_micros: timestamp_micros(log.timestamp.as_ref()),
        fields: log.fields.iter().map(key_value_from_proto).collect(),
    }
}
