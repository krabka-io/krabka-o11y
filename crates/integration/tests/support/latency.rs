//! Exact latency quantiles, and how steady they were across the phase.

use std::time::Duration;

use num_traits::ToPrimitive as _;
use serde_json::{Value, json};

/// The width of one window in the coefficient of variation.
pub const CV_WINDOW: Duration = Duration::from_secs(5);

/// Every latency one phase observed, with when in the phase it started.
///
/// The samples are kept whole rather than bucketed. A bucketed quantile moves
/// in steps of two or more, and a gate at 1.5 times its baseline cannot read
/// a step that large.
#[derive(Clone, Debug, Default)]
pub struct Latencies {
    samples: Vec<(Duration, u64)>,
}

impl Latencies {
    /// Records one call that started `at` into the measured window and took
    /// `elapsed`.
    pub fn observe(&mut self, at: Duration, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.samples.push((at, micros));
    }

    pub fn count(&self) -> u64 {
        u64::try_from(self.samples.len()).unwrap_or(u64::MAX)
    }

    /// The nearest-rank quantile `numerator / denominator`, in microseconds.
    pub fn quantile(&self, numerator: usize, denominator: usize) -> Option<u64> {
        let mut sorted: Vec<u64> = self.samples.iter().map(|(_, micros)| *micros).collect();
        sorted.sort_unstable();
        let rank = (sorted.len() * numerator).div_ceil(denominator).max(1);
        sorted.get(rank - 1).copied()
    }

    /// The coefficient of variation of the per-window mean latency, and the
    /// number of windows it was computed over.
    ///
    /// A phase shorter than two windows has no variation to report, so the
    /// value is `None` and the gate treats the phase as unmeasured noise.
    pub fn window_cv(&self) -> (Option<f64>, usize) {
        let mut windows: Vec<(u64, u64)> = Vec::new();
        for (at, micros) in &self.samples {
            let index = usize::try_from(at.as_millis() / CV_WINDOW.as_millis()).unwrap_or(0);
            if windows.len() <= index {
                windows.resize(index + 1, (0, 0));
            }
            windows[index].0 = windows[index].0.saturating_add(*micros);
            windows[index].1 += 1;
        }
        let means: Vec<f64> = windows
            .iter()
            .filter(|(_, count)| *count > 0)
            .map(|(sum, count)| as_f64(*sum) / as_f64(*count))
            .collect();
        if means.len() < 2 {
            return (None, means.len());
        }
        let n = as_f64(u64::try_from(means.len()).unwrap_or(u64::MAX));
        let mean = means.iter().sum::<f64>() / n;
        if mean <= 0.0 {
            return (None, means.len());
        }
        let variance = means.iter().map(|m| (m - mean).powi(2)).sum::<f64>() / n;
        (Some(variance.sqrt() / mean), means.len())
    }

    /// The report form: count, quantiles in microseconds, and the window CV.
    pub fn summary(&self) -> Value {
        let (cv, windows) = self.window_cv();
        json!({
            "count": self.count(),
            "p50": self.quantile(50, 100),
            "p95": self.quantile(95, 100),
            "p99": self.quantile(99, 100),
            "max": self.quantile(1, 1),
            "cv": cv,
            "cv_windows": windows,
            "cv_window_seconds": CV_WINDOW.as_secs(),
        })
    }
}

/// A count as a float, for a rate or a ratio.
pub fn as_f64(count: u64) -> f64 {
    count.to_f64().unwrap_or(f64::MAX)
}

/// `numerator / denominator`, or zero when nothing was counted.
pub fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        as_f64(numerator) / as_f64(denominator)
    }
}
