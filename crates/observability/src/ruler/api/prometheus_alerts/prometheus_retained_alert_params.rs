use super::{BTreeSet, Labels, PrometheusAlertKey};

pub(crate) struct PrometheusRetainedAlertParams<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) alert_name: &'a str,
    pub(crate) query: &'a str,
    pub(crate) evaluation_time: i64,
    pub(crate) hold_duration_ns: i64,
    pub(crate) keep_firing_for_ns: i64,
    pub(crate) active_keys: &'a BTreeSet<PrometheusAlertKey>,
    pub(crate) annotation_templates: &'a Labels,
}
