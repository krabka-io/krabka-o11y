use super::{Arc, CancellationToken, DistributorState, SocketAddr, handle_jaeger_compact_datagram};

/// Serve the Jaeger compact-Thrift UDP receiver until cancelled, returning the
/// bound address and the receive loop's handle.
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
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
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
                                handle_jaeger_compact_datagram(&state, "anonymous", &buf[..len]).await
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
    Ok((bound, handle))
}
