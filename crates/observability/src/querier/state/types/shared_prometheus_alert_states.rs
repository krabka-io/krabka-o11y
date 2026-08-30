use super::{Arc, BTreeMap, Mutex, PrometheusAlertKey, PrometheusAlertRuntimeState};

#[derive(Clone, Default)]
pub(crate) struct SharedPrometheusAlertStates {
    pub(crate) alerts: Arc<Mutex<BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>>>,
}

impl SharedPrometheusAlertStates {
    pub(crate) fn clear_tenant(&self, tenant: &str) {
        self.alerts
            .lock()
            .expect("Prometheus alert state lock poisoned")
            .retain(|key, _| key.tenant != tenant);
    }
}
