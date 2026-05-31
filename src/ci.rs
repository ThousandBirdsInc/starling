//! CI mode exit conditions (Tilt's `tilt ci`).
//!
//! `starling ci` brings the project up once and waits for it to settle, then
//! exits 0 (everything came up) or non-zero (something failed / timed out).
//! This module holds the pure decision — given the current resource statuses,
//! is the session still settling, done, or failed — so it can be tested without
//! running the engine. The command wiring lives in `main.rs`.

use crate::api::v1alpha1::UIResource;

/// Parse a Go-style duration (`"30m"`, `"90s"`, `"1h30m"`) or a bare integer
/// (seconds) into whole seconds. Returns `None` if it cannot be parsed. Used for
/// `ci_settings(timeout=...)`.
pub fn parse_duration_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<u64>() {
        return Some(n); // bare number = seconds
    }
    let mut total = 0u64;
    let mut num = String::new();
    for c in s.chars() {
        if c.is_ascii_digit() {
            num.push(c);
            continue;
        }
        if num.is_empty() {
            return None;
        }
        let n: u64 = num.parse().ok()?;
        total += match c {
            's' => n,
            'm' => n * 60,
            'h' => n * 3600,
            _ => return None,
        };
        num.clear();
    }
    if !num.is_empty() {
        return None; // trailing digits with no unit
    }
    Some(total)
}

/// The state of a CI run, derived from the resources' update/runtime statuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CiOutcome {
    /// At least one resource is still building or not yet ready.
    Pending,
    /// Every enabled resource finished updating and reached a settled runtime.
    Done,
    /// At least one resource's update or runtime errored.
    Failed,
}

/// An update status that counts as finished-successfully for CI.
fn update_settled_ok(status: &str) -> bool {
    matches!(status, "ok" | "not_applicable")
}

/// A runtime status that counts as settled for CI (ready, or nothing to run).
fn runtime_settled_ok(status: &str) -> bool {
    matches!(status, "ok" | "not_applicable" | "none")
}

/// Whether a resource is disabled (so CI ignores it).
fn is_disabled(r: &UIResource) -> bool {
    r.status
        .as_ref()
        .and_then(|s| s.disable_status.as_ref())
        .map(|d| d.state == "Disabled")
        .unwrap_or(false)
}

/// Decide the CI outcome for a set of resources. Disabled resources are ignored.
/// Any errored update/runtime fails the run; otherwise the run is `Done` only
/// once every enabled resource has settled, and `Pending` until then.
pub fn ci_outcome(resources: &[UIResource]) -> CiOutcome {
    let mut all_settled = true;
    for r in resources {
        if is_disabled(r) {
            continue;
        }
        let status = r.status.as_ref();
        let update = status
            .and_then(|s| s.update_status.as_deref())
            .unwrap_or("none");
        let runtime = status
            .and_then(|s| s.runtime_status.as_deref())
            .unwrap_or("none");

        if update == "error" || runtime == "error" {
            return CiOutcome::Failed;
        }
        if !(update_settled_ok(update) && runtime_settled_ok(runtime)) {
            all_settled = false;
        }
    }
    if all_settled {
        CiOutcome::Done
    } else {
        CiOutcome::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::v1alpha1::{DisableResourceStatus, UIResourceStatus};

    fn res(update: &str, runtime: &str) -> UIResource {
        UIResource {
            metadata: None,
            spec: None,
            status: Some(UIResourceStatus {
                update_status: Some(update.to_string()),
                runtime_status: Some(runtime.to_string()),
                ..Default::default()
            }),
        }
    }

    fn disabled(update: &str, runtime: &str) -> UIResource {
        let mut r = res(update, runtime);
        r.status.as_mut().unwrap().disable_status = Some(DisableResourceStatus {
            enabled_count: 0,
            disabled_count: 1,
            state: "Disabled".to_string(),
            sources: vec![],
        });
        r
    }

    #[test]
    fn pending_until_all_settle() {
        // One built, one still building.
        let rs = vec![res("ok", "ok"), res("in_progress", "pending")];
        assert_eq!(ci_outcome(&rs), CiOutcome::Pending);
        // A not-yet-started resource (default "none") is still pending.
        assert_eq!(ci_outcome(&[res("none", "none")]), CiOutcome::Pending);
    }

    #[test]
    fn done_when_all_enabled_resources_settled() {
        let rs = vec![
            res("ok", "ok"),
            res("not_applicable", "none"), // a local one-shot that exited
            res("ok", "not_applicable"),
        ];
        assert_eq!(ci_outcome(&rs), CiOutcome::Done);
    }

    #[test]
    fn failed_on_any_error() {
        assert_eq!(
            ci_outcome(&[res("ok", "ok"), res("error", "none")]),
            CiOutcome::Failed
        );
        assert_eq!(ci_outcome(&[res("ok", "error")]), CiOutcome::Failed);
    }

    #[test]
    fn disabled_resources_are_ignored() {
        // The only non-settled resource is disabled -> Done.
        let rs = vec![res("ok", "ok"), disabled("none", "none")];
        assert_eq!(ci_outcome(&rs), CiOutcome::Done);
        // A disabled, errored resource does not fail the run.
        let rs = vec![res("ok", "ok"), disabled("error", "error")];
        assert_eq!(ci_outcome(&rs), CiOutcome::Done);
    }

    #[test]
    fn empty_is_done() {
        assert_eq!(ci_outcome(&[]), CiOutcome::Done);
    }

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration_secs("30m"), Some(1800));
        assert_eq!(parse_duration_secs("90s"), Some(90));
        assert_eq!(parse_duration_secs("1h30m"), Some(5400));
        assert_eq!(parse_duration_secs("300"), Some(300));
        assert_eq!(parse_duration_secs("2h"), Some(7200));
        assert_eq!(parse_duration_secs(""), None);
        assert_eq!(parse_duration_secs("bad"), None);
        assert_eq!(parse_duration_secs("10x"), None);
        assert_eq!(parse_duration_secs("5m30"), None); // trailing unitless
    }
}
