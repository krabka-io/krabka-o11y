use super::PprofProfile;

/// The metric name Pyroscope gives a pprof uploaded through `/ingest`.
///
/// A pprof carries its own sample types but no metric name. Pyroscope does not
/// use the `?name=` application for one, because that becomes `service_name`.
/// It derives the name from the first sample type instead. Observed against
/// `grafana/pyroscope:2.2.1` by uploading its own `/debug/pprof/*` profiles:
/// `goroutine` becomes `goroutines`, every heap sample type becomes `memory`,
/// and both `contentions` and `delay` become `mutex`. A Go block profile
/// becomes `mutex` too, because its sample types are the same as a mutex
/// profile's.
pub(crate) fn pprof_metric_name(profile: &PprofProfile) -> Option<String> {
    let (sample_type, _) = profile.sample_types().into_iter().next()?;
    let name = match sample_type.as_str() {
        "goroutine" => "goroutines",
        "alloc_objects" | "alloc_space" | "inuse_objects" | "inuse_space" => "memory",
        "contentions" | "delay" => "mutex",
        "cpu" | "samples" => "process_cpu",
        other => other,
    };
    Some(name.to_string())
}
