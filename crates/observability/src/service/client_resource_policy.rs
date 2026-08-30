/// Validated Kafka connection resource limits shared by this process.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClientResourcePolicy {
    pub dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    pub frame_max: krabka_client_core::ClientFrameMax,
}
