use super::{
    IngestEnforcer, KeyValue, Limits, TracesError, limit_error_to_traces_error,
    shared_attr_measured,
};

pub(crate) fn check_shared_attrs(limits: &Limits, attrs: &[KeyValue]) -> Result<(), TracesError> {
    let flattened = attrs.iter().map(shared_attr_measured).collect::<Vec<_>>();
    IngestEnforcer::check_attributes(limits, &flattened)
        .map_err(|err| limit_error_to_traces_error(&err))
}
