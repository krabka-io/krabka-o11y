use super::Cli;

pub(crate) struct ClientResourcePolicy {
    pub(crate) client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    pub(crate) client_frame_max: krabka_client_core::ClientFrameMax,
}

pub(crate) fn client_resource_policy(cli: &Cli) -> ClientResourcePolicy {
    ClientResourcePolicy {
        client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated profiles client dispatch queue capacity"),
        client_frame_max: krabka_client_core::ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated profiles client frame maximum"),
    }
}
