//! Complete uncached frontend responses with warmed tenant manifest indexes.

use std::{hint::black_box, time::Instant};

use krabka_o11y_benches::{log_frontend::LogFrontendFixture, profile_run_args::ProfileRunArgs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let ProfileRunArgs {
        streams,
        iterations,
        case: name,
    } = ProfileRunArgs::take_from(&mut args)?;
    let runtime = tokio::runtime::Runtime::new()?;
    let shards = args.next().is_some_and(|source| source == "shards");
    let fixture = runtime.block_on(async {
        if shards {
            LogFrontendFixture::new_shards(streams).await
        } else {
            LogFrontendFixture::new(streams).await
        }
    });
    runtime.block_on(fixture.verify(&name))?;
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(runtime.block_on(fixture.execute(&name))?);
    }
    println!(
        "streams={streams} iterations={iterations} case={name} elapsed_seconds={}",
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}
