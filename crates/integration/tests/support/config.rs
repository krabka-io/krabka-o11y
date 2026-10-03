//! The soak's fixed shape: phase lengths, dataset, load levels, objectives.
//!
//! Every number here is part of the qualification shape in
//! `docs/operating_envelope.md`. Only the phase length and the objectives read
//! the environment, so a stable-runner run and a shared-runner run differ in
//! how long they measure and what they call a pass, never in what they do.

use std::time::Duration;

use serde_json::{Value, json};

/// The phase length when `KRABKA_SOAK_PHASE_SECONDS` is unset.
///
/// Twelve seconds per phase keeps the default run of all four signals near
/// eight minutes, and gives each measured phase two whole 5 s windows for its
/// coefficient of variation.
pub const DEFAULT_PHASE_SECONDS: u64 = 12;

/// The dataset seed when `KRABKA_SOAK_SEED` is unset.
pub const DEFAULT_SEED: u64 = 267;

#[derive(Clone, Debug)]
pub struct Config {
    /// The measured length of one phase.
    pub phase: Duration,
    /// The unmeasured load before each phase. One quarter of the phase.
    pub warmup: Duration,
    pub seed: u64,
    /// Series per batch in the steady, compaction, deletion and noisy phases.
    pub series: u32,
    /// Points per series per batch.
    pub samples: u32,
    /// The gap between two batches from one paced writer.
    pub write_interval: Duration,
    /// Concurrent unpaced writers at each burst level.
    pub burst_levels: Vec<u32>,
    /// Series per batch at each step of the high-cardinality ramp.
    pub cardinality_steps: Vec<u32>,
    /// How far back a query reads, except in the cold-block and restart phases.
    pub query_window: Duration,
    /// How far back the cold-block and restart phases read: every block the
    /// earlier phases wrote.
    pub full_window: Duration,
    /// The retention the deletion phase enforces while it writes.
    pub retention_secs: u32,
    /// The gap between two maintenance passes during load.
    pub maintenance_interval: Duration,
    /// Unpaced writers on the noisy tenant.
    pub noisy_writers: u32,
    /// The noisy tenant's ingest limit, in rows per second.
    pub noisy_rows_per_sec: u32,
    /// The longest a restart may take before its first query sees data.
    pub recovery_timeout: Duration,
    pub objectives: Objectives,
}

/// What a phase must meet for its load level to count as sustained.
#[derive(Clone, Copy, Debug)]
pub struct Objectives {
    pub write_p99: Duration,
    pub query_p99: Duration,
    /// Errors over attempts. A rejection by the rate limiter is not an error.
    pub max_error_rate: f64,
}

impl Config {
    pub fn from_env() -> Self {
        let phase_seconds = env_u64("KRABKA_SOAK_PHASE_SECONDS").unwrap_or(DEFAULT_PHASE_SECONDS);
        let phase = Duration::from_secs(phase_seconds.max(1));
        let objectives = Objectives {
            write_p99: Duration::from_millis(env_u64("KRABKA_SOAK_WRITE_P99_MS").unwrap_or(2_000)),
            query_p99: Duration::from_millis(env_u64("KRABKA_SOAK_QUERY_P99_MS").unwrap_or(2_000)),
            max_error_rate: 0.0,
        };
        Self {
            phase,
            warmup: phase / 4,
            seed: env_u64("KRABKA_SOAK_SEED").unwrap_or(DEFAULT_SEED),
            series: 100,
            samples: 10,
            write_interval: Duration::from_millis(250),
            burst_levels: vec![1, 2, 4, 8],
            cardinality_steps: vec![100, 1_000, 5_000],
            query_window: Duration::from_secs(30),
            full_window: Duration::from_mins(30),
            retention_secs: 60,
            maintenance_interval: Duration::from_secs(2),
            noisy_writers: 4,
            noisy_rows_per_sec: 2_000,
            recovery_timeout: Duration::from_mins(1),
            objectives,
        }
    }

    /// The report form, so a result names the shape it was measured under.
    pub fn report(&self) -> Value {
        json!({
            "phase_seconds": self.phase.as_secs_f64(),
            "warmup_seconds": self.warmup.as_secs_f64(),
            "write_interval_ms": duration_ms(self.write_interval),
            "burst_levels": self.burst_levels,
            "cardinality_steps": self.cardinality_steps,
            "query_window_seconds": self.query_window.as_secs(),
            "full_window_seconds": self.full_window.as_secs(),
            "retention_seconds": self.retention_secs,
            "maintenance_interval_ms": duration_ms(self.maintenance_interval),
            "noisy_writers": self.noisy_writers,
            "noisy_rows_per_sec": self.noisy_rows_per_sec,
            "recovery_timeout_seconds": self.recovery_timeout.as_secs(),
        })
    }

    /// The dataset every run writes, and the seed that fixes its values.
    pub fn dataset(&self) -> Value {
        json!({
            "seed": self.seed,
            "series_per_batch": self.series,
            "points_per_series": self.samples,
            "tenants": super::TENANTS,
            "metrics": "one float series per `series` label on `soak_samples`; values from the seed",
            "logs": "one stream per `series` label under app=api, env=prod; one line per point",
            "traces": "one trace per series, one span per point, service.name=api",
            "profiles": "one process_cpu profile per series, one sample per point, service_name=api",
            "timestamps": "wall clock at write time, so retention and query windows are real",
        })
    }
}

impl Objectives {
    pub fn report(&self) -> Value {
        json!({
            "write_p99_us": duration_us(self.write_p99),
            "query_p99_us": duration_us(self.query_p99),
            "max_error_rate": self.max_error_rate,
        })
    }
}

pub fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub fn duration_us(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name}={value} is not a whole number")),
    )
}

/// A deterministic value for point `point` of series `series` in batch `seq`.
///
/// `SplitMix64` over the four inputs: cheap, and the same run with the same
/// seed writes the same values.
pub fn mix(seed: u64, seq: i64, series: u32, point: u32) -> u64 {
    let mut z = seed
        ^ seq.unsigned_abs().rotate_left(17)
        ^ u64::from(series).rotate_left(34)
        ^ u64::from(point).rotate_left(51);
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}
