use super::*;

#[cfg(feature = "experimental-functions")]
pub(crate) fn sample_instances(samples: &[crate::InstantSample]) -> Vec<&str> {
    let mut instances = samples
        .iter()
        .map(|sample| sample.labels.get("instance").expect("instance label"))
        .collect::<Vec<_>>();
    instances.sort_unstable();
    instances
}
