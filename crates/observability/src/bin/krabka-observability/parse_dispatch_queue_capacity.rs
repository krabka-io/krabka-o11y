pub(crate) fn parse_dispatch_queue_capacity(value: &str) -> Result<usize, String> {
    let value = value.parse::<usize>().map_err(|error| error.to_string())?;
    krabka_client_core::ConnectionDispatchQueueCapacity::new(value)
        .map(krabka_client_core::ConnectionDispatchQueueCapacity::get)
}
