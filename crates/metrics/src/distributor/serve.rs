krabka_observability::define_distributor_serve!(
    super::DistributorState,
    super::router,
    "metrics distributor",
    "Binds and serves the metrics distributor until `shutdown` resolves. `security` controls TLS and authentication. The default serves plain HTTP without authentication. Returns an error when the address cannot be bound or queried."
);
