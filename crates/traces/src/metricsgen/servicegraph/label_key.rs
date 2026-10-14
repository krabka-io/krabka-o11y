use super::ConnectionType;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct LabelKey {
    pub(crate) client: String,
    pub(crate) server: String,
    pub(crate) connection_type: ConnectionType,
    pub(crate) dimensions: Vec<krabka_traceql::MetricLabel>,
}
