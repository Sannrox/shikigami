//! Optional process metrics (JSON snapshot). No Prometheus dependency by default.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;

/// Cumulative counters for fleet operators.
#[derive(Debug, Default)]
pub struct Metrics {
    pub runs_total: AtomicU64,
    pub runs_success: AtomicU64,
    pub runs_failed: AtomicU64,
    pub runs_parked: AtomicU64,
    pub turns_total: AtomicU64,
    pub plane_errors: AtomicU64,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn record_run(&self, success: bool, parked: bool, turns: u32) {
        self.runs_total.fetch_add(1, Ordering::Relaxed);
        self.turns_total.fetch_add(turns as u64, Ordering::Relaxed);
        if parked {
            self.runs_parked.fetch_add(1, Ordering::Relaxed);
        } else if success {
            self.runs_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.runs_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_plane_error(&self) {
        self.plane_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            runs_total: self.runs_total.load(Ordering::Relaxed),
            runs_success: self.runs_success.load(Ordering::Relaxed),
            runs_failed: self.runs_failed.load(Ordering::Relaxed),
            runs_parked: self.runs_parked.load(Ordering::Relaxed),
            turns_total: self.turns_total.load(Ordering::Relaxed),
            plane_errors: self.plane_errors.load(Ordering::Relaxed),
        }
    }
}

/// Serializable metrics export (JSON).
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub runs_total: u64,
    pub runs_success: u64,
    pub runs_failed: u64,
    pub runs_parked: u64,
    pub turns_total: u64,
    pub plane_errors: u64,
}

impl MetricsSnapshot {
    /// Prometheus text exposition (optional operator export without extra crates).
    pub fn to_prometheus(&self) -> String {
        format!(
            concat!(
                "# HELP shikigami_runs_total Total runs attempted\n",
                "# TYPE shikigami_runs_total counter\n",
                "shikigami_runs_total {}\n",
                "# HELP shikigami_runs_success_total Successful runs\n",
                "# TYPE shikigami_runs_success_total counter\n",
                "shikigami_runs_success_total {}\n",
                "# HELP shikigami_runs_failed_total Failed runs\n",
                "# TYPE shikigami_runs_failed_total counter\n",
                "shikigami_runs_failed_total {}\n",
                "# HELP shikigami_runs_parked_total Parked runs\n",
                "# TYPE shikigami_runs_parked_total counter\n",
                "shikigami_runs_parked_total {}\n",
                "# HELP shikigami_turns_total Model turns completed\n",
                "# TYPE shikigami_turns_total counter\n",
                "shikigami_turns_total {}\n",
                "# HELP shikigami_plane_errors_total Plane/governance errors observed\n",
                "# TYPE shikigami_plane_errors_total counter\n",
                "shikigami_plane_errors_total {}\n",
            ),
            self.runs_total,
            self.runs_success,
            self.runs_failed,
            self.runs_parked,
            self.turns_total,
            self.plane_errors,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_and_prometheus_export() {
        let m = Metrics::new();
        m.record_run(true, false, 3);
        m.record_run(false, true, 1);
        m.record_plane_error();
        let s = m.snapshot();
        assert_eq!(s.runs_total, 2);
        assert_eq!(s.runs_success, 1);
        assert_eq!(s.runs_parked, 1);
        assert_eq!(s.turns_total, 4);
        assert_eq!(s.plane_errors, 1);
        let text = s.to_prometheus();
        assert!(text.contains("shikigami_runs_total 2"));
        assert!(text.contains("shikigami_plane_errors_total 1"));
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"runs_total\":2"));
    }
}
