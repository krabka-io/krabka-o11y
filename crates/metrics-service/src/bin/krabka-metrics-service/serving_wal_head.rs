use super::{
    Cli, ClientSecurity, ReadinessGate, RoleReadiness, WalHead, WalHeadConsumerRecovery,
    WalHeadFeed,
};

/// The WAL head a serving role tops its compacted blocks up with.
pub(crate) struct ServingWalHead {
    pub(crate) head: WalHead,
    /// The head and the readiness gate it opens, when the role is configured
    /// to read the WAL.
    pub(crate) status: Option<(WalHead, ReadinessGate)>,
    consumer: Option<ServingWalConsumer>,
}

/// What the role's WAL head consumer reads from and reports to, when the role
/// is configured to read the WAL.
struct ServingWalConsumer {
    bootstrap: String,
    gate: ReadinessGate,
    recovery: WalHeadConsumerRecovery,
}

impl ServingWalHead {
    /// Creates the head with the CLI's retention and registers its consumer
    /// with the role's readiness. A role configured to read the WAL is not
    /// ready until it does: until then its answers stop at the last compacted
    /// block and silently omit everything since.
    pub(crate) fn open(
        cli: &Cli,
        metrics: &krabka_promql::metrics::ServiceMetrics,
        readiness: &RoleReadiness,
    ) -> Self {
        let head = WalHead::with_retention(cli.wal_head_retention);
        readiness.track_wal_consumer(metrics.wal_consumer.clone());
        let consumer = cli
            .wal_bootstrap
            .clone()
            .map(|bootstrap| ServingWalConsumer {
                bootstrap,
                gate: readiness.gate("wal-head"),
                recovery: WalHeadConsumerRecovery::for_serving_role(
                    metrics.wal_consumer.clone(),
                    readiness,
                ),
            });
        let status = consumer
            .as_ref()
            .map(|consumer| (head.clone(), consumer.gate.clone()));
        Self {
            head,
            status,
            consumer,
        }
    }

    /// The feed for the role's WAL head consumer in `group_id`, or `None` when
    /// the role does not read the WAL.
    pub(crate) fn take_feed(
        &mut self,
        security: Option<ClientSecurity>,
        group_id: String,
    ) -> Option<WalHeadFeed> {
        self.consumer.take().map(|consumer| WalHeadFeed {
            bootstrap: consumer.bootstrap,
            security,
            group_id,
            head: self.head.clone(),
            gate: consumer.gate,
            recovery: consumer.recovery,
        })
    }
}
