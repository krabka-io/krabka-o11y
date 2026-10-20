use super::{
    DistributorState, Principal, ReceiverEndpoint, RunningServer, TracesError,
    decode_jaeger_thrift, produce_spans,
};

/// Serve the Jaeger compact-Thrift UDP receiver until cancelled, returning the
/// bound address and the receive loop's handle, or `None` when `security`
/// turns on authentication.
///
/// A datagram carries no header and no TLS, so it cannot present a
/// credential. When a credentials file is configured, the receiver does not
/// bind its port and logs one warning that says why. Tempo's own UDP receiver
/// has no authentication either. With no credentials file, the receiver serves
/// every datagram as unauthenticated, as upstream does.
///
/// UDP has no connection for a failure to show up on, so a receiver that has
/// stopped looks exactly like a client that is not sending. The handle is how
/// the caller tells the two apart: supervise it, and a receive loop that ends
/// -- on a socket error, or on a panic decoding one datagram -- ends the role
/// instead of silently dropping every span sent over this port.
///
/// # Errors
/// Returns an error when the socket cannot be bound.
pub async fn serve_jaeger_compact_udp(
    endpoint: ReceiverEndpoint<'_>,
) -> std::io::Result<Option<RunningServer>> {
    let ReceiverEndpoint {
        addr,
        state,
        security,
        shutdown,
    } = endpoint;
    if security.authentication_enabled() {
        tracing::warn!(
            %addr,
            "traces distributor does not start the Jaeger compact UDP receiver: a datagram cannot carry a credential, and --auth-credentials-config turns on authentication"
        );
        return Ok(None);
    }
    let socket = tokio::net::UdpSocket::bind(addr).await?;
    let bound = socket.local_addr()?;
    let handle = tokio::spawn(async move {
        let mut buf = vec![0_u8; 65_535];
        loop {
            tokio::select! {
                () = shutdown.cancelled() => break,
                received = socket.recv_from(&mut buf) => {
                    match received {
                        Ok((len, peer)) => {
                            if let Err(err) =
                                handle_jaeger_compact_datagram(&state, &buf[..len]).await
                            {
                                tracing::warn!(%peer, error = %err, "jaeger compact datagram rejected");
                            }
                        }
                        Err(err) => {
                            tracing::error!(error = %err, "jaeger compact UDP receiver stopped");
                            break;
                        }
                    }
                }
            }
        }
    });
    Ok(Some(RunningServer {
        bound_addr: bound,
        task: handle,
    }))
}

pub(crate) async fn handle_jaeger_compact_datagram(
    state: &DistributorState,
    body: &[u8],
) -> Result<(), TracesError> {
    // A datagram has no header, so it names no tenant. The policy decides:
    // the anonymous policy stores it as the anonymous tenant, and a policy
    // that requires a tenant rejects it.
    //
    // A datagram also carries no credential. The receiver runs only when
    // authentication is off, and then every request is unauthenticated.
    let tenant = state.resolve_tenant(&Principal::Unauthenticated, None)?;
    let spans = decode_jaeger_thrift(body)?;
    state.enforce_ingest(&tenant, &spans)?;
    produce_spans(state.sink.as_ref(), &tenant, spans).await
}
