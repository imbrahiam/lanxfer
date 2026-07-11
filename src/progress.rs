//! Shared live-transfer state: workers write, UI ticks read.
//! One instance per outgoing transfer; one global instance for everything
//! the local server receives.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Default)]
pub struct Progress {
    total_bytes: AtomicU64,
    done_bytes: AtomicU64,
    total_files: AtomicU64,
    done_files: AtomicU64,
    /// In-flight units keyed by (file id | stripe): one entry per data
    /// connection worker.
    active: Mutex<HashMap<u64, Unit>>,
}

struct Unit {
    label: String,
    done: u64,
    total: u64,
}

/// Point-in-time copy for rendering.
pub struct Snapshot {
    pub total_bytes: u64,
    pub done_bytes: u64,
    pub total_files: u64,
    pub done_files: u64,
    /// (label, done, total) per in-flight unit / connection.
    pub units: Vec<(String, u64, u64)>,
}

/// Key for a transfer unit: whole file or one stripe.
pub fn unit_key(id: u32, stripe: Option<u32>) -> u64 {
    (id as u64) | ((stripe.map(|s| s + 1).unwrap_or(0) as u64) << 32)
}

impl Progress {
    /// Zero all counters if nothing is in flight — called when a new session
    /// starts so finished history doesn't skew the numbers.
    pub fn reset_if_idle(&self) {
        let active = self.active.lock().unwrap();
        if active.is_empty() {
            self.total_bytes.store(0, Ordering::Relaxed);
            self.done_bytes.store(0, Ordering::Relaxed);
            self.total_files.store(0, Ordering::Relaxed);
            self.done_files.store(0, Ordering::Relaxed);
        }
    }

    pub fn add_totals(&self, bytes: u64, files: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_files.fetch_add(files, Ordering::Relaxed);
    }

    pub fn begin_unit(&self, key: u64, label: String, total: u64) {
        self.active.lock().unwrap().insert(
            key,
            Unit {
                label,
                done: 0,
                total,
            },
        );
    }

    pub fn advance(&self, key: u64, bytes: u64) {
        self.done_bytes.fetch_add(bytes, Ordering::Relaxed);
        if let Some(unit) = self.active.lock().unwrap().get_mut(&key) {
            unit.done += bytes;
        }
    }

    pub fn end_unit(&self, key: u64) {
        self.active.lock().unwrap().remove(&key);
    }

    pub fn file_done(&self) {
        self.done_files.fetch_add(1, Ordering::Relaxed);
    }

    /// Something is being transferred right now.
    pub fn is_active(&self) -> bool {
        !self.active.lock().unwrap().is_empty()
    }

    pub fn snapshot(&self) -> Snapshot {
        let mut units: Vec<_> = self
            .active
            .lock()
            .unwrap()
            .values()
            .map(|u| (u.label.clone(), u.done, u.total))
            .collect();
        units.sort_by(|a, b| a.0.cmp(&b.0));
        Snapshot {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            done_bytes: self.done_bytes.load(Ordering::Relaxed),
            total_files: self.total_files.load(Ordering::Relaxed),
            done_files: self.done_files.load(Ordering::Relaxed),
            units,
        }
    }
}

/// Exponentially-smoothed throughput from successive done-byte readings.
pub struct SpeedGauge {
    last: Option<(Instant, u64)>,
    ema: f64,
    last_files: Option<(Instant, u64)>,
    file_ema: f64,
}

impl Default for SpeedGauge {
    fn default() -> Self {
        Self {
            last: None,
            ema: 0.0,
            last_files: None,
            file_ema: 0.0,
        }
    }
}

impl SpeedGauge {
    /// Feed the current cumulative byte count; returns bytes/second.
    pub fn update(&mut self, done_bytes: u64) -> f64 {
        let now = Instant::now();
        if let Some((prev_t, prev_b)) = self.last {
            let dt = now.duration_since(prev_t).as_secs_f64();
            if dt >= 0.05 {
                let inst = (done_bytes.saturating_sub(prev_b)) as f64 / dt;
                // ponytail: fixed 0.3 smoothing; make adaptive if jumpy
                self.ema = if self.ema == 0.0 {
                    inst
                } else {
                    0.3 * inst + 0.7 * self.ema
                };
                self.last = Some((now, done_bytes));
            }
        } else {
            self.last = Some((now, done_bytes));
        }
        self.ema
    }

    /// Smoothed completed-files/second. Combined with byte throughput for a
    /// folder-wide ETA that accounts for per-file overhead on small files.
    pub fn update_files(&mut self, done_files: u64) -> f64 {
        let now = Instant::now();
        if let Some((prev_t, prev_files)) = self.last_files {
            let dt = now.duration_since(prev_t).as_secs_f64();
            if dt >= 0.05 {
                let inst = done_files.saturating_sub(prev_files) as f64 / dt;
                if inst > 0.0 {
                    self.file_ema = if self.file_ema == 0.0 {
                        inst
                    } else {
                        0.3 * inst + 0.7 * self.file_ema
                    };
                }
                self.last_files = Some((now, done_files));
            }
        } else {
            self.last_files = Some((now, done_files));
        }
        self.file_ema
    }
}

/// Overall transfer ETA. Byte throughput models large files; file throughput
/// models filesystem and protocol overhead for folders with many small files.
/// Taking the slower estimate prevents the current active file from making a
/// whole-folder transfer appear nearly finished.
pub fn overall_eta(
    remaining_bytes: u64,
    bytes_per_second: f64,
    remaining_files: u64,
    files_per_second: f64,
) -> String {
    if remaining_bytes == 0 && remaining_files == 0 {
        return "—".to_string();
    }
    let byte_secs = (bytes_per_second >= 1.0).then(|| remaining_bytes as f64 / bytes_per_second);
    let file_secs = (files_per_second > 0.0).then(|| remaining_files as f64 / files_per_second);
    match (byte_secs, file_secs) {
        (Some(bytes), Some(files)) => format_eta_seconds(bytes.max(files)),
        (Some(bytes), None) => format_eta_seconds(bytes),
        (None, Some(files)) => format_eta_seconds(files),
        (None, None) => "—".to_string(),
    }
}

fn format_eta_seconds(seconds: f64) -> String {
    let secs = seconds.round() as u64;
    if secs >= 3600 {
        format!("{}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60)
    } else {
        format!("{}:{:02}", secs / 60, secs % 60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_tracks_units_and_totals() {
        let p = Progress::default();
        p.add_totals(100, 2);
        p.begin_unit(unit_key(0, None), "a".into(), 60);
        p.advance(unit_key(0, None), 60);
        p.end_unit(unit_key(0, None));
        p.file_done();
        let s = p.snapshot();
        assert_eq!((s.done_bytes, s.total_bytes, s.done_files), (60, 100, 1));
        assert!(!p.is_active());
        // reset only when idle
        p.reset_if_idle();
        assert_eq!(p.snapshot().total_bytes, 0);
        assert_eq!(overall_eta(0, 100.0, 0, 0.0), "—");
        assert_eq!(overall_eta(0, 0.0, 50, 5.0), "0:10");
        assert_eq!(overall_eta(720_000, 100.0, 0, 0.0), "2:00:00");
        assert_eq!(overall_eta(500, 100.0, 50, 5.0), "0:10");
        assert_eq!(overall_eta(500, 100.0, 0, 0.0), "0:05");
    }
}
