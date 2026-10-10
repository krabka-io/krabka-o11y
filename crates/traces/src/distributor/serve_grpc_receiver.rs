use tonic::service::Routes;

use super::{
    Arc, DistributorState, GrpcAuthenticationLayer, GrpcServer, ReceiverEndpoint, SocketAddr,
    bind_listener, grpc_incoming, spawn_server,
};

/// One gRPC receiver: the routes it serves over the distributor state, and the
/// name its server logs under.
pub(crate) struct GrpcReceiver {
    pub(crate) routes: fn(Arc<DistributorState>) -> Routes,
    pub(crate) server_name: &'static str,
}

/// Serve `receiver` on `endpoint` until cancelled, returning the bound address
/// and the server's handle.
pub(crate) async fn serve_grpc_receiver(
    endpoint: ReceiverEndpoint<'_>,
    receiver: GrpcReceiver,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let ReceiverEndpoint {
        addr,
        state,
        security,
        shutdown,
    } = endpoint;
    let listener = bind_listener(addr, security).await?;
    let bound = listener.local_addr();
    let server = GrpcServer::builder()
        .layer(GrpcAuthenticationLayer::new(security))
        .add_routes((receiver.routes)(state))
        .serve_with_incoming_shutdown(grpc_incoming(listener), shutdown.cancelled_owned());
    Ok((bound, spawn_server(server, receiver.server_name)))
}
