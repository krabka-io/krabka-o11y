use std::{
    io::{self, IoSlice},
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;

use super::{PeerAddr, ServerStream};

/// A connection from [`grpc_incoming`](super::grpc_incoming), with the peer it came from.
///
/// tonic puts its [`PeerAddr`] into the extensions of every request on the
/// connection, where [`GrpcAuthenticationLayer`](super::GrpcAuthenticationLayer)
/// reads the verified client certificate.
#[derive(Debug)]
pub struct GrpcConnection {
    stream: ServerStream,
    peer: PeerAddr,
}

impl GrpcConnection {
    pub(super) fn new(stream: ServerStream, peer: PeerAddr) -> Self {
        Self { stream, peer }
    }
}

impl Connected for GrpcConnection {
    type ConnectInfo = PeerAddr;

    fn connect_info(&self) -> Self::ConnectInfo {
        self.peer.clone()
    }
}

impl AsyncRead for GrpcConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for GrpcConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
