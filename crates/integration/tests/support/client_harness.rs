#[path = "../captured_requests/mod.rs"]
mod captured_requests;
#[path = "container_image.rs"]
mod container_image;

pub use captured_requests::{expect_seen, spawn_capture_server};
pub use container_image::{AsyncRunner, GenericImage, ImageExt, WaitFor};

macro_rules! capture_server {
    ($seen:expr, $router:expr, $capture:expr, $spawn:expr) => {{
        let seen = $seen;
        let app = ($router).fallback($capture).with_state(Arc::clone(&seen));
        let port = ($spawn)(app).await?;
        (seen, port)
    }};
}

pub(crate) use capture_server;
