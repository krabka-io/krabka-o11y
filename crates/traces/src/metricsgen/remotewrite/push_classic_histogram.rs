use super::*;

pub(crate) fn push_classic_histogram(
    out: &mut Vec<WireTimeSeries>,
    s: &Series,
    buckets: &[(f64, f64)],
    sum: f64,
    count: f64,
) {
    let bucket_name = format!("{}_bucket", s.name);
    let mut assigned_exemplars = vec![false; s.exemplars.len()];

    for (le, cumulative) in buckets {
        let mut labels = s.labels.clone();
        labels.push(("le".to_string(), le_label(*le)));
        let exemplars =
            bucket_exemplars(&s.exemplars, &mut assigned_exemplars, |ex| ex.value <= *le);
        out.push(WireTimeSeries {
            labels: with_name(&bucket_name, &labels),
            value: *cumulative,
            timestamp_ms: s.timestamp_ms,
            exemplars,
            native_histogram: None,
        });
    }

    let mut inf_labels = s.labels.clone();
    inf_labels.push(("le".to_string(), "+Inf".to_string()));
    let inf_exemplars = bucket_exemplars(&s.exemplars, &mut assigned_exemplars, |_| true);
    out.push(WireTimeSeries {
        labels: with_name(&bucket_name, &inf_labels),
        value: count,
        timestamp_ms: s.timestamp_ms,
        exemplars: inf_exemplars,
        native_histogram: None,
    });
    out.push(WireTimeSeries {
        labels: with_name(&format!("{}_sum", s.name), &s.labels),
        value: sum,
        timestamp_ms: s.timestamp_ms,
        exemplars: Vec::new(),
        native_histogram: None,
    });
    out.push(WireTimeSeries {
        labels: with_name(&format!("{}_count", s.name), &s.labels),
        value: count,
        timestamp_ms: s.timestamp_ms,
        exemplars: Vec::new(),
        native_histogram: None,
    });
}
