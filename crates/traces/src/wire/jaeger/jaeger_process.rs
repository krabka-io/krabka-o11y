use super::KeyValue;

#[derive(Clone, Default)]
pub(crate) struct JaegerProcess {
    pub(crate) service_name: String,
    pub(crate) tags: Vec<KeyValue>,
}
