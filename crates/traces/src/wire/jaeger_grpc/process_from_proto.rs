use super::{JaegerProcess, api_v2, key_value_from_proto};

pub(crate) fn process_from_proto(process: &api_v2::Process) -> JaegerProcess {
    JaegerProcess {
        service_name: process.service_name.clone(),
        tags: process.tags.iter().map(key_value_from_proto).collect(),
    }
}
