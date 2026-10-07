use assert2::assert;
use krabka_client_consumer::ConsumerRecord;
use krabka_metrics_service::WalHeadConsumerError;
use krabka_observability::RoleReadiness;
use krabka_units::prelude::millis;
use tokio::sync::oneshot;

use super::*;

struct ClosingConsumer {
    shutdown: Shutdown,
    fail_poll: bool,
    closing: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

#[async_trait::async_trait]
impl WalHeadConsumerPoll for ClosingConsumer {
    async fn poll(&mut self, _: Time) -> Result<Vec<ConsumerRecord>, WalHeadConsumerError> {
        self.shutdown.trigger();
        if self.fail_poll {
            Err(WalHeadConsumerError::Poll("injected poll failure".into()))
        } else {
            Ok(Vec::new())
        }
    }

    async fn close(self) -> Result<(), WalHeadConsumerError> {
        self.closing.send(()).unwrap();
        self.release.await.unwrap();
        Ok(())
    }
}

#[async_trait::async_trait]
impl WalHeadConsumerCommit for ClosingConsumer {
    async fn commit_sync(&mut self) -> Result<(), WalHeadConsumerError> {
        Ok(())
    }
}

#[tokio::test]
async fn the_role_waits_for_consumer_close_after_shutdown_and_poll_failure() {
    for fail_poll in [false, true] {
        let shutdown = Shutdown::new();
        let readiness = RoleReadiness::new();
        let (closing, closed) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let consumer = ClosingConsumer {
            shutdown: shutdown.clone(),
            fail_poll,
            closing,
            release: released,
        };
        let task = spawn_wal_head_consumer_task(
            || async { Ok(consumer) },
            WalHead::new(),
            "wal".into(),
            millis(1),
            shutdown,
            readiness.gate("wal-head"),
            WalHeadConsumerRecovery {
                metrics: None,
                catch_up_gate: Some(readiness.gate("wal-catch-up")),
            },
        );
        tokio::time::timeout(std::time::Duration::from_secs(30), closed)
            .await
            .expect("the role starts consumer close")
            .expect("consumer close reports entry");
        assert!(!task.is_finished());
        assert!(readiness.pending() == vec!["wal-head".to_string(), "wal-catch-up".to_string()]);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), task)
            .await
            .expect("the role finishes after consumer close")
            .expect("the role joins");
    }
}
