use super::CancellationToken;

pub(crate) fn role_shutdown_token() -> CancellationToken {
    let token = CancellationToken::new();
    let signal = token.clone();
    tokio::spawn(async move {
        krabka_observability::shutdown_signal().await;
        signal.cancel();
    });
    token
}
