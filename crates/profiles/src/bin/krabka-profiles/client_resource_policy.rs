use super::Cli;

pub(crate) fn client_resource_policy(
    cli: &Cli,
) -> (
    krabka_client_core::ConnectionDispatchQueueCapacity,
    krabka_client_core::ClientFrameMax,
) {
    (
        krabka_client_core::ConnectionDispatchQueueCapacity::new(
            cli.client_dispatch_queue_capacity,
        )
        .expect("validated profiles client dispatch queue capacity"),
        krabka_client_core::ClientFrameMax::try_from(cli.client_frame_max)
            .expect("validated profiles client frame maximum"),
    )
}
