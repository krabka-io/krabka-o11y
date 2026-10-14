krabka_observability::define_distributor_serve!(
    super::DistributorState,
    super::router,
    "profiles distributor",
    "Serves the distributor on `addr` until `shutdown` completes and returns the bound address. `security` controls TLS and authentication. The default serves plain HTTP and accepts every request. Returns an error when the address cannot be bound or queried."
);
