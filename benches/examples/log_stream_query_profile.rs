//! Complete cold log queries for CPU, allocation, and repeated wall-clock captures.

use std::{hint::black_box, sync::Arc, time::Instant};

use krabka_o11y_benches::log_queries::LogQueryFixture;
use krabka_observability::execute_stream_query_from_object_store;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let streams = args.next().expect("stream count").parse::<usize>()?;
    let iterations = args.next().expect("iteration count").parse::<usize>()?;
    let name = args.next().expect("query case");
    let runtime = tokio::runtime::Runtime::new()?;
    let fixture = runtime.block_on(LogQueryFixture::new(streams));
    let case = fixture.case(&name);
    let actual = runtime.block_on(execute_stream_query_from_object_store(
        Arc::clone(&fixture.store),
        &fixture.prefix,
        &case.plan,
        &fixture.label_index,
    ))?;
    if actual != case.expected {
        return Err("query differs from the complete fixture ledger".into());
    }
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(runtime.block_on(execute_stream_query_from_object_store(
            Arc::clone(&fixture.store),
            &fixture.prefix,
            &case.plan,
            &fixture.label_index,
        ))?);
    }
    println!(
        "streams={streams} iterations={iterations} case={name} selected={} elapsed_seconds={}",
        case.plan.fingerprints.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
