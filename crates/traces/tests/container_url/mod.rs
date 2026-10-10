// The host-side URL of a port a test container publishes.

use testcontainers::{ContainerAsync, GenericImage, TestcontainersError};

/// The `http://127.0.0.1:<port>` base URL that reaches `port` of `container`
/// from the host.
pub async fn mapped_base_url(
    container: &ContainerAsync<GenericImage>,
    port: u16,
) -> Result<String, TestcontainersError> {
    let mapped = container.get_host_port_ipv4(port).await?;
    Ok(format!("http://127.0.0.1:{mapped}"))
}
