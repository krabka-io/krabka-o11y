use super::*;

struct SetOpSample {
    instance: &'static str,
    value: f64,
}

pub(crate) fn set_op_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        SetOpSample {
            instance: "a",
            value: 1.0,
        },
        SetOpSample {
            instance: "b",
            value: 2.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            10_000,
            sample.value,
        );
    }
    for sample in [
        SetOpSample {
            instance: "b",
            value: 20.0,
        },
        SetOpSample {
            instance: "c",
            value: 30.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "target_info"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
                crate::test_support::TestLabelPair::new("region", "east"),
            ]),
            10_000,
            sample.value,
        );
    }
    store
}
