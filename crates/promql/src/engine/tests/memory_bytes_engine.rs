use super::*;

#[derive(bon::Builder)]
struct MemoryBytesSample {
    instance: &'static str,
    value: f64,
}

/// An engine over `memory_bytes` for instances `a` = 1, `b` = 3 and `c` = 2 at 10s.
pub(crate) fn memory_bytes_engine() -> PromqlEngine<InMemoryMetricStore> {
    let mut store = FloatStore::default();
    for sample in [
        MemoryBytesSample::builder()
            .instance("a")
            .value(1.0)
            .build(),
        MemoryBytesSample::builder()
            .instance("b")
            .value(3.0)
            .build(),
        MemoryBytesSample::builder()
            .instance("c")
            .value(2.0)
            .build(),
    ] {
        store = store.sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            sample.value,
        );
    }
    store.engine()
}

/// One `memory_bytes{job, instance}` float sample.
pub(crate) struct JobInstanceSample {
    pub(crate) job: &'static str,
    pub(crate) instance: &'static str,
    pub(crate) value: f64,
}

/// An engine over `memory_bytes{job, instance}` holding each of `samples` at 10s.
pub(crate) fn job_memory_bytes_engine(
    samples: &[JobInstanceSample],
) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    for sample in samples {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
                crate::test_support::TestLabelPair::new("job", sample.job),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            10_000,
            sample.value,
        );
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}

/// Whether `samples` holds `expected` with its `memory_bytes` name and its
/// original `job` and `instance` labels.
pub(crate) fn has_job_memory_bytes_sample(
    samples: &[crate::InstantSample],
    expected: &JobInstanceSample,
) -> bool {
    samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some(expected.job)
            && sample.labels.get("instance") == Some(expected.instance)
            && approx_eq(float_value(&sample.value), expected.value)
    })
}
