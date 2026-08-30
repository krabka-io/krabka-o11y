use super::{Message, Value, WebSocket};

pub(crate) async fn send_tail_frame(socket: &mut WebSocket, frame: Value) -> bool {
    socket
        .send(Message::Text(frame.to_string().into()))
        .await
        .is_ok()
}
