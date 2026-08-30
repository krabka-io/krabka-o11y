use super::*;

pub(crate) fn set_op_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "up"), ("instance", instance), ("job", "api")]),
            10_000,
            value,
        );
    }
    for (instance, value) in [("b", 20.0), ("c", 30.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "target_info"),
                ("instance", instance),
                ("region", "east"),
            ]),
            10_000,
            value,
        );
    }
    store
}
