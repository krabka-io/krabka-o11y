use super::*;

pub(crate) fn merge_samples(existing: &mut Vec<MetricSample>, incoming: Vec<MetricSample>) {
    for sample in incoming {
        if let Some(found) = existing
            .iter_mut()
            .find(|s| s.timestamp_ms == sample.timestamp_ms)
        {
            found.value += sample.value;
        } else {
            existing.push(sample);
        }
    }
    existing.sort_by(|a, b| {
        let ka = a.timestamp_ms.parse::<i128>().unwrap_or(i128::MAX);
        let kb = b.timestamp_ms.parse::<i128>().unwrap_or(i128::MAX);
        ka.cmp(&kb)
            .then_with(|| a.timestamp_ms.cmp(&b.timestamp_ms))
    });
}
