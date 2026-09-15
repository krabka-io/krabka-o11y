use krabka_blockstore::ObjectStoreMetrics;
use serde::Serialize;

use crate::wal_consumer_metrics::{WalConsumerMetrics, WalRecoveryStatus};

use super::{Extension, IntoResponse, Response, RoleReadiness};

#[derive(Serialize)]
pub(crate) struct RecoveryStatus {
    ready: bool,
    pending: Vec<String>,
    wal_consumers: Vec<WalRecoveryStatus>,
    last_successful_object_store_operation_unix_millis: Option<i64>,
}

impl RecoveryStatus {
    pub(crate) fn from_parts(
        readiness: &RoleReadiness,
        wal_consumers: &[WalConsumerMetrics],
        object_stores: &[ObjectStoreMetrics],
    ) -> Self {
        let pending = readiness.pending();
        Self {
            ready: pending.is_empty(),
            pending,
            wal_consumers: wal_consumers
                .iter()
                .map(WalConsumerMetrics::recovery_status)
                .collect(),
            last_successful_object_store_operation_unix_millis: object_stores
                .iter()
                .filter_map(ObjectStoreMetrics::last_success_unix_millis)
                .max(),
        }
    }
}

pub(crate) async fn recovery_status(Extension(readiness): Extension<RoleReadiness>) -> Response {
    axum::Json(readiness.recovery_status()).into_response()
}
