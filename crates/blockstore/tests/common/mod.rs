use krabka_blockstore::{LabelPair, LogLabels, labels};

pub fn api_prod_labels() -> LogLabels {
    labels([LabelPair::new("app", "api"), LabelPair::new("env", "prod")])
}

pub fn worker_prod_labels() -> LogLabels {
    labels([
        LabelPair::new("app", "worker"),
        LabelPair::new("env", "prod"),
    ])
}
