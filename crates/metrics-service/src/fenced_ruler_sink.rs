use std::sync::Arc;

use krabka_blockstore::{Labels, TenantId};
use krabka_metrics::WalRecord;
use krabka_promql::{
    AlertmanagerAlert, AlertmanagerSink, RecordingRuleWalSink, RulerAlertStateRecord,
    RulerGroupStateRecord, RulerStateSink, RulerWalError,
};

use crate::RulerFence;

pub struct FencedRulerSink<S> {
    inner: S,
    fence: Arc<RulerFence>,
}

impl<S> FencedRulerSink<S> {
    #[must_use]
    pub fn new(inner: S, fence: Arc<RulerFence>) -> Self {
        Self { inner, fence }
    }
}

#[async_trait::async_trait]
impl<S: RecordingRuleWalSink> RecordingRuleWalSink for FencedRulerSink<S> {
    async fn append_recording_rule_record(&self, record: WalRecord) -> Result<(), RulerWalError> {
        self.fence.check()?;
        self.inner.append_recording_rule_record(record).await
    }
}

#[async_trait::async_trait]
impl<S: RulerStateSink> RulerStateSink for FencedRulerSink<S> {
    async fn persist_ruler_group_state(
        &self,
        record: RulerGroupStateRecord,
    ) -> Result<(), RulerWalError> {
        self.fence.check()?;
        self.inner.persist_ruler_group_state(record).await
    }

    async fn persist_ruler_alert_state(
        &self,
        record: RulerAlertStateRecord,
    ) -> Result<(), RulerWalError> {
        self.fence.check()?;
        self.inner.persist_ruler_alert_state(record).await
    }
}

#[async_trait::async_trait]
impl<S: AlertmanagerSink> AlertmanagerSink for FencedRulerSink<S> {
    fn template_external_labels(&self) -> Labels {
        self.inner.template_external_labels()
    }

    fn template_external_url(&self, alert_name: &str) -> String {
        self.inner.template_external_url(alert_name)
    }

    async fn dispatch_alerts(&self, alerts: Vec<AlertmanagerAlert>) -> Result<(), RulerWalError> {
        self.fence.check()?;
        self.inner.dispatch_alerts(alerts).await
    }

    async fn dispatch_alerts_for_tenant(
        &self,
        tenant: &TenantId,
        alerts: Vec<AlertmanagerAlert>,
    ) -> Result<(), RulerWalError> {
        self.fence.check()?;
        self.inner.dispatch_alerts_for_tenant(tenant, alerts).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::assert;
    use krabka_metrics::SamplePayload;

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingSink(Arc<AtomicUsize>);

    impl RecordingSink {
        fn record(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl RecordingRuleWalSink for RecordingSink {
        async fn append_recording_rule_record(
            &self,
            _record: WalRecord,
        ) -> Result<(), RulerWalError> {
            self.record();
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl RulerStateSink for RecordingSink {
        async fn persist_ruler_group_state(
            &self,
            _record: RulerGroupStateRecord,
        ) -> Result<(), RulerWalError> {
            self.record();
            Ok(())
        }

        async fn persist_ruler_alert_state(
            &self,
            _record: RulerAlertStateRecord,
        ) -> Result<(), RulerWalError> {
            self.record();
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl AlertmanagerSink for RecordingSink {
        async fn dispatch_alerts(
            &self,
            _alerts: Vec<AlertmanagerAlert>,
        ) -> Result<(), RulerWalError> {
            self.record();
            Ok(())
        }
    }

    #[tokio::test]
    async fn every_output_is_rejected_without_the_current_lease() {
        let inner = RecordingSink::default();
        let calls = Arc::clone(&inner.0);
        let fence = Arc::new(RulerFence::new(std::time::Duration::from_secs(30)));
        let sink = FencedRulerSink::new(inner, Arc::clone(&fence));
        let wal = WalRecord {
            tenant: "tenant-a".into(),
            labels: vec![("__name__".into(), "up".into())],
            payload: SamplePayload::Float {
                timestamp_ms: 1,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let group = RulerGroupStateRecord {
            tenant: "tenant-a".into(),
            namespace: "default".into(),
            group: "group".into(),
            last_eval_ms: 1,
        };
        let alert = RulerAlertStateRecord {
            tenant: "tenant-a".into(),
            rule_id: "rule".into(),
            labels: std::collections::BTreeMap::new(),
            active_since_ms: Some(1),
            keep_firing_until_ms: None,
        };

        assert!(
            sink.append_recording_rule_record(wal.clone())
                .await
                .is_err()
        );
        assert!(sink.persist_ruler_group_state(group.clone()).await.is_err());
        assert!(sink.persist_ruler_alert_state(alert.clone()).await.is_err());
        assert!(sink.dispatch_alerts(Vec::new()).await.is_err());
        assert!(calls.load(Ordering::SeqCst) == 0);

        fence.renew("member-a", 7);
        sink.append_recording_rule_record(wal).await.unwrap();
        sink.persist_ruler_group_state(group).await.unwrap();
        sink.persist_ruler_alert_state(alert).await.unwrap();
        sink.dispatch_alerts(Vec::new()).await.unwrap();
        assert!(calls.load(Ordering::SeqCst) == 4);

        fence.revoke();
        assert!(sink.dispatch_alerts(Vec::new()).await.is_err());
        assert!(calls.load(Ordering::SeqCst) == 4);
    }
}
