use tonic::service::Routes;

use super::{
    Arc, DistributorState, GrpcAuthenticationLayer, GrpcServer, ReceiverEndpoint, RunningServer,
    bind_and_spawn_server, grpc_incoming,
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
) -> std::io::Result<RunningServer> {
    let ReceiverEndpoint {
        addr,
        state,
        security,
        shutdown,
    } = endpoint;
    bind_and_spawn_server(
        addr,
        security,
        |listener| {
            GrpcServer::builder()
                .layer(GrpcAuthenticationLayer::new(security))
                .add_routes((receiver.routes)(state))
                .serve_with_incoming_shutdown(grpc_incoming(listener), shutdown.cancelled_owned())
        },
        receiver.server_name,
    )
    .await
}
