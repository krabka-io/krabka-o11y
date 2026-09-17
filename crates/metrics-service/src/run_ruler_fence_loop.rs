use std::sync::Arc;

use krabka_client_consumer::Consumer;
use krabka_units::Time;

use crate::RulerFence;

pub async fn run_ruler_fence_loop<Stop>(
    mut consumer: Consumer,
    fence: Arc<RulerFence>,
    topic: String,
    partition: i32,
    poll_timeout: Time,
    stop: Stop,
) where
    Stop: std::future::Future<Output = ()>,
{
    tokio::pin!(stop);
    loop {
        let polled = tokio::select! {
            () = &mut stop => break,
            result = consumer.poll(poll_timeout) => result,
        };
        let valid = match polled {
            Ok(_) => {
                consumer.commit_sync().await.is_ok()
                    && consumer
                        .assignment()
                        .await
                        .iter()
                        .any(|assigned| assigned == &(topic.clone(), partition))
            }
            Err(error) => {
                tracing::warn!(%error, "ruler fence poll failed");
                false
            }
        };
        if valid {
            fence.renew(consumer.member_id(), consumer.generation_id());
        } else {
            fence.revoke();
        }
    }
    fence.revoke();
}
