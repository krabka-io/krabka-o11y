use super::*;

pub(crate) fn labels(metric: &str, job: &str) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", metric);
    labels.insert("job", job);
    labels
}
