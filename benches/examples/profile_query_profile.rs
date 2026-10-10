//! CPU and allocation captures of full profile queries after fixture creation.

use std::{hint::black_box, sync::Arc, time::Instant};

use krabka_o11y_benches::{
    index::TENANT,
    profile_samples::{PROFILE_TYPE, profile_samples},
};
use krabka_pprof::{
    EngineOpts, FlameEngine, FlameGraph, InMemoryProfileStore, ProfileError, SampleSelector,
};

#[inline(never)]
async fn resolve_profile(
    engine: &FlameEngine<InMemoryProfileStore>,
    end_ms: i64,
    selector: SampleSelector<'_>,
    sites: &[String],
) -> Result<FlameGraph, ProfileError> {
    engine
        .select_merge_stacktraces_with_selectors(
            (TENANT, PROFILE_TYPE, "{service=\"api\"}"),
            (0, end_ms),
            i64::MAX,
            sites,
            selector,
        )
        .await
        .map(black_box)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let samples = args.next().expect("sample count").parse::<usize>()?;
    let iterations = args.next().expect("iteration count").parse::<usize>()?;
    let case = args.next().expect("query case");
    let fixture = profile_samples(samples, 64);
    let engine = FlameEngine::new(Arc::new(fixture.store), EngineOpts::default());
    let call_sites = ["main".to_string()];
    let (selector, sites, expected) = match case.as_str() {
        "grouped" => (SampleSelector::None, &[][..], &fixture.all),
        "trace_all" => (
            SampleSelector::Trace(&fixture.traces),
            &[][..],
            &fixture.all,
        ),
        "trace_one" => (
            SampleSelector::Trace(&fixture.traces[..1]),
            &[][..],
            &fixture.one_trace,
        ),
        "trace_callsite" => (
            SampleSelector::Trace(&fixture.traces),
            &call_sites[..],
            &fixture.main,
        ),
        _ => return Err("unknown query case".into()),
    };
    let runtime = tokio::runtime::Runtime::new()?;
    let actual = runtime.block_on(resolve_profile(&engine, fixture.end_ms, selector, sites))?;
    if actual != *expected {
        return Err("query differs from the complete fixture ledger".into());
    }
    let started = Instant::now();
    for _ in 0..iterations {
        black_box(runtime.block_on(resolve_profile(&engine, fixture.end_ms, selector, sites))?);
    }
    println!(
        "samples={samples} iterations={iterations} case={case} expected_total={} elapsed_seconds={}",
        expected.total,
        started.elapsed().as_secs_f64()
    );
    Ok(())
}
