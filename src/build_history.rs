//! Durable history of the Docker image builds Starling performs.
//!
//! Every Dockerfile build is profiled (wall-clock duration) and appended as a
//! JSON line to a single history file under the Starling state dir, so the
//! history survives instance restarts and can be reviewed from the TUI.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// One profiled Docker image build.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DockerBuildRecord {
    /// The image reference built (e.g. `app-agent-grpc:dev`).
    pub image_ref: String,
    /// The resource whose build triggered it.
    pub resource: String,
    /// The project (the Starlingfile/Tiltfile's directory name) it belongs to.
    pub project: String,
    /// RFC3339 start time.
    pub started_at: String,
    /// Wall-clock build duration in milliseconds.
    pub duration_ms: u64,
    /// Whether the build succeeded.
    pub success: bool,
    /// Size of the build context, in bytes (0 when unknown — e.g. a
    /// `custom_build` command rather than a Dockerfile build).
    pub context_bytes: u64,
}

impl DockerBuildRecord {
    /// Human-readable duration, e.g. `1m04s` or `12.3s` or `840ms`.
    pub fn duration_human(&self) -> String {
        let ms = self.duration_ms;
        if ms < 1000 {
            format!("{ms}ms")
        } else if ms < 60_000 {
            format!("{:.1}s", ms as f64 / 1000.0)
        } else {
            let secs = ms / 1000;
            format!("{}m{:02}s", secs / 60, secs % 60)
        }
    }

    /// Human-readable context size, e.g. `2.0 GB` or `230.4 MB` or `512 KB`.
    pub fn context_human(&self) -> String {
        let b = self.context_bytes as f64;
        if self.context_bytes == 0 {
            "-".to_string()
        } else if b >= 1024.0 * 1024.0 * 1024.0 {
            format!("{:.1} GB", b / (1024.0 * 1024.0 * 1024.0))
        } else if b >= 1024.0 * 1024.0 {
            format!("{:.1} MB", b / (1024.0 * 1024.0))
        } else {
            format!("{:.0} KB", b / 1024.0)
        }
    }
}

/// The project name for a config path (its parent directory's name).
pub fn project_for(config_path: &Path) -> String {
    std::fs::canonicalize(config_path)
        .ok()
        .as_deref()
        .unwrap_or(config_path)
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("app")
        .to_string()
}

/// The JSONL history file (one record per line), under the Starling state dir.
pub fn history_path() -> PathBuf {
    crate::daemon::protocol::state_dir().join("docker-builds.jsonl")
}

/// Append a record to the history file. Best-effort: failures are ignored so a
/// build is never failed by history bookkeeping.
pub fn append(record: &DockerBuildRecord) {
    let path = history_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(line) = serde_json::to_string(record) else {
        return;
    };
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        let _ = writeln!(f, "{line}");
    }
}

/// The most recent `limit` records, newest first. Empty if the history file
/// doesn't exist yet or can't be read/parsed.
pub fn recent(limit: usize) -> Vec<DockerBuildRecord> {
    let Ok(text) = std::fs::read_to_string(history_path()) else {
        return vec![];
    };
    let mut records: Vec<DockerBuildRecord> = text
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect();
    records.reverse();
    records.truncate(limit);
    records
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(ms: u64, ok: bool, bytes: u64) -> DockerBuildRecord {
        DockerBuildRecord {
            image_ref: "img:dev".to_string(),
            resource: "grpc".to_string(),
            project: "app".to_string(),
            started_at: "2026-01-01T00:00:00Z".to_string(),
            duration_ms: ms,
            success: ok,
            context_bytes: bytes,
        }
    }

    #[test]
    fn formats_duration_and_context() {
        assert_eq!(rec(840, true, 0).duration_human(), "840ms");
        assert_eq!(rec(12_340, true, 0).duration_human(), "12.3s");
        assert_eq!(rec(64_000, true, 0).duration_human(), "1m04s");
        assert_eq!(rec(0, true, 0).context_human(), "-");
        assert_eq!(rec(0, true, 230 * 1024 * 1024).context_human(), "230.0 MB");
        assert_eq!(
            rec(0, true, 2 * 1024 * 1024 * 1024).context_human(),
            "2.0 GB"
        );
    }
}
