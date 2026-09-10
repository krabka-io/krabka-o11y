use super::{PprofProfile, intern_profile_string};

/// The metric name Pyroscope gives every stack-shaped legacy upload.
///
/// The `/ingest` door's stack formats (`groups`, `lines`, `trie`, `tree`,
/// `speedscope`) carry no sample type of their own, so Pyroscope's original
/// ingestion pipeline gives them all the same one: a CPU profile whose values
/// are wall-clock nanoseconds. Observed against `grafana/pyroscope:2.2.1`,
/// which answers a folded upload with
/// `process_cpu:cpu:nanoseconds:cpu:nanoseconds`.
pub(crate) const LEGACY_CPU_METRIC_NAME: &str = "process_cpu";

/// Re-type a stack-shaped legacy profile as Pyroscope's default CPU profile.
///
/// The converters build a profile of raw sample counts. Pyroscope reads those
/// counts as samples taken at `sample_rate` Hz and stores the time they stand
/// for, so one count becomes `1s / sample_rate` nanoseconds: at the default
/// 100 Hz a count of 100 is one second, or 1e9. The sampling interval is also
/// the profile's `period`.
pub(crate) fn apply_legacy_cpu_mapping(profile: PprofProfile, sample_rate: u32) -> PprofProfile {
    let mut profile = profile.into_inner();
    let period = (1_000_000_000_i64 / i64::from(sample_rate)).max(1);
    profile.period = period;
    let type_ref = intern_profile_string(&mut profile.string_table, "cpu");
    let unit_ref = intern_profile_string(&mut profile.string_table, "nanoseconds");
    let value_type = krabka_pprof::proto::ValueType {
        r#type: type_ref,
        unit: unit_ref,
    };
    profile.sample_type = vec![value_type];
    profile.period_type = Some(value_type);
    profile.default_sample_type = type_ref;
    for sample in &mut profile.sample {
        // One value per sample: the converters emit single-valued samples, and
        // `sample_type` above is now a single entry, so anything beyond the
        // first would no longer have a type to belong to.
        sample.value.truncate(1);
        for value in &mut sample.value {
            *value = value.saturating_mul(period);
        }
    }
    PprofProfile::from(profile)
}
