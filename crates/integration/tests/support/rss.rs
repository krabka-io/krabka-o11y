//! Resident memory, sampled while a phase runs.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use serde_json::{Value, json};

const SAMPLE_INTERVAL: Duration = Duration::from_millis(250);

/// The process's resident set, in kibibytes, from `/proc/self/status`.
pub fn resident_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status")
        .expect("/proc/self/status is readable; this suite runs on Linux");
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib = rest
                .split_whitespace()
                .next()
                .expect("VmRSS carries a number");
            return kib.parse().expect("VmRSS is a number of kibibytes");
        }
    }
    panic!("/proc/self/status carries no VmRSS line")
}

/// The host's memory, in kibibytes, from `/proc/meminfo`.
pub fn host_memory_kib() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|kib| kib.parse().ok())
}

/// Samples resident memory on an interval until it is stopped.
pub struct RssSampler {
    start: u64,
    peak: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl RssSampler {
    pub fn start() -> Self {
        let start = resident_kib();
        let peak = Arc::new(AtomicU64::new(start));
        let stop = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let peak = Arc::clone(&peak);
            let stop = Arc::clone(&stop);
            async move {
                while !stop.load(Ordering::Relaxed) {
                    peak.fetch_max(resident_kib(), Ordering::Relaxed);
                    tokio::time::sleep(SAMPLE_INTERVAL).await;
                }
            }
        });
        Self {
            start,
            peak,
            stop,
            task,
        }
    }

    /// Stops sampling and returns start, peak and end, in kibibytes.
    pub async fn finish(self) -> Value {
        self.stop.store(true, Ordering::Relaxed);
        self.task.await.expect("the RSS sampler does not panic");
        let end = resident_kib();
        let peak = self.peak.load(Ordering::Relaxed).max(end);
        json!({ "start": self.start, "peak": peak, "end": end })
    }
}
