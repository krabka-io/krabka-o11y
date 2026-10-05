//! Container startup shared by the logs and metrics deployment suites.
use std::{os::unix::fs::MetadataExt as _, time::Duration};

use testcontainers::{
    ContainerAsync, ContainerRequest, GenericImage, ImageExt as _,
    core::{Healthcheck, Mount, WaitFor, logs::LogFrame, wait::ExitWaitStrategy},
    runners::AsyncRunner as _,
};
pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
const START_TIMEOUT: Duration = Duration::from_mins(2);
pub fn image(name: &str) -> GenericImage {
    let variable = format!("KRABKA_{name}_IMAGE_REF");
    // The wrapper records Docker's content ID after loading the tarball.
    // Use it so another worktree loading the dev tag cannot change this case.
    let reference = std::env::var(format!("KRABKA_{name}_IMAGE_ID"))
        .or_else(|_| std::env::var(&variable))
        .unwrap_or_else(|_| panic!("{variable} is set by `bazel test --config=docker`"));
    let (repository, tag) = reference
        .rsplit_once(':')
        .expect("image reference has a tag or content ID");
    GenericImage::new(repository, tag)
}

pub async fn start(
    request: ContainerRequest<GenericImage>,
) -> TestResult<ContainerAsync<GenericImage>> {
    let command = request.cmd().collect::<Vec<_>>().join(" ");
    let request = request.with_log_consumer(move |frame: &LogFrame| {
        eprintln!("[{command}] {}", String::from_utf8_lossy(frame.bytes()));
    });
    Ok(tokio::time::timeout(START_TIMEOUT, request.start()).await??)
}

pub async fn start_broker(
    directory: &std::path::Path,
    network: &str,
) -> TestResult<ContainerAsync<GenericImage>> {
    let broker_name = format!("{network}-broker");
    let metadata = directory.metadata()?;
    // The formatter and broker write as the directory owner, so a failed
    // case leaves no root-owned files that TempDir cannot remove.
    let user = format!("{}:{}", metadata.uid(), metadata.gid());
    let mount = Mount::bind_mount(directory.to_string_lossy(), "/data");
    let formatter = start(
        image("BROKER")
            .with_entrypoint("/usr/bin/krabka-format")
            .with_wait_for(WaitFor::exit(ExitWaitStrategy::new().with_exit_code(0)))
            .with_network(network)
            .with_user(&user)
            .with_mount(mount.clone())
            .with_cmd([
                "--log-dir=/data".to_string(),
                "--standalone".to_string(),
                "--node-id=1".to_string(),
                format!("--controller-listener={broker_name}:9093"),
            ]),
    )
    .await?;
    let broker = start(
        image("BROKER")
            .with_wait_for(WaitFor::healthcheck())
            .with_network(network)
            .with_container_name(&broker_name)
            .with_user(user)
            .with_mount(mount)
            .with_health_check(
                Healthcheck::cmd([
                    "/usr/bin/krabka-guard",
                    "--bootstrap-server",
                    "127.0.0.1:9092",
                    "freeze",
                    "list",
                ])
                .with_interval(Duration::from_secs(1))
                .with_timeout(Duration::from_secs(3))
                .with_start_period(Duration::from_secs(60)),
            )
            .with_cmd([
                "--log-dir=/data".to_string(),
                "--listen-addr=0.0.0.0:9092".to_string(),
                format!("--advertised-listener={broker_name}:9092"),
                "--process-roles=controller,broker".to_string(),
                "--offsets-topic-replication-factor=1".to_string(),
            ]),
    )
    .await?;
    drop(formatter);
    let bootstrap = start(
        image("KRABKA")
            .with_wait_for(WaitFor::exit(ExitWaitStrategy::new().with_exit_code(0)))
            .with_network(network)
            .with_cmd([
                "krabka-o11y-bootstrap".to_string(),
                format!("--bootstrap={broker_name}:9092"),
                "--partitions=1".to_string(),
                "--state-partitions=1".to_string(),
                "--replicas=1".to_string(),
            ]),
    )
    .await?;
    drop(bootstrap);
    Ok(broker)
}

pub async fn base_url(container: &ContainerAsync<GenericImage>, port: u16) -> TestResult<String> {
    Ok(format!(
        "http://{}:{}",
        container.get_host().await?,
        container.get_host_port_ipv4(port).await?
    ))
}
