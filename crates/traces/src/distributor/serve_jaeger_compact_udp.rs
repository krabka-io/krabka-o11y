use super::{SocketAddr, Arc, DistributorState, CancellationToken, handle_jaeger_compact_datagram};

/// Serve the Jaeger compact-Thrift UDP receiver until cancelled.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn serve_jaeger_compact_udp(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> std::io::Result<SocketAddr> {
    let socket = tokio::net::UdpSocket::bind(addr).await?;
    let bound = socket.local_addr()?;
    tokio::spawn(async move {
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
                            shutdown.cancel();
                            break;
                        }
                    }
                }
            }
        }
    });
    Ok(bound)
}
