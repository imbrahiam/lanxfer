use anyhow::{Context, Result};
use std::time::Duration;

use crate::picker::{StatusScreen, Tone};

const OWNER: &str = "imbrahiam";
const REPO: &str = "lanxfer";

/// Maximum attempts for the download+install step (rate-limit retries).
const MAX_ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(5);

/// Friendly platform slug embedded in release asset names
/// (`lanxfer-<tag>-<slug>.<ext>`). Must match the `name` values in
/// `.github/workflows/release.yml`; `self_update` picks the asset whose
/// name contains this string, so the two have to stay in sync.
const fn asset_slug() -> &'static str {
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        "linux-x86_64"
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        "windows-x86_64"
    }
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        "macos-arm64"
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        "macos-x86_64"
    }
}

pub fn run(check_only: bool, assume_yes: bool) -> Result<()> {
    let installed = env!("CARGO_PKG_VERSION");

    // ── Phase 1: TUI — version check + confirmation prompt ───────────
    let mut screen = StatusScreen::new()?;
    screen.render(
        "Update",
        "Checking GitHub Releases…",
        Tone::Info,
        &[("installed".into(), installed.into())],
        "Connecting to github.com",
    )?;

    let releases = self_update::backends::github::ReleaseList::configure()
        .repo_owner(OWNER)
        .repo_name(REPO)
        .build()
        .context("could not configure update check")?
        .fetch()
        .context("could not fetch GitHub releases")?;
    let latest = releases
        .first()
        .context("no GitHub releases published yet")?;
    let latest_version = latest.version.trim_start_matches('v');
    let versions = vec![
        ("installed".into(), installed.into()),
        ("latest".into(), latest_version.into()),
    ];

    if !self_update::version::bump_is_greater(installed, &latest.version)? {
        screen.render(
            "Update",
            "Already up to date",
            Tone::Success,
            &versions,
            "enter / esc  close",
        )?;
        screen.wait_for_close()?;
        return Ok(());
    }
    if check_only {
        screen.render(
            "Update",
            "Update available",
            Tone::Warning,
            &versions,
            "Run `lanxfer update` to install  ·  enter / esc close",
        )?;
        screen.wait_for_close()?;
        return Ok(());
    }

    if !assume_yes {
        let choice = screen.choose(
            &format!("Update {installed} → {latest_version}"),
            vec!["Download and install".into(), "Cancel".into()],
            0,
            "↑↓ move · enter select · esc cancel",
        )?;
        if choice != Some(0) {
            return Ok(());
        }
    }

    // ── Phase 2: Console — download + install outside the TUI ─────────
    screen.suspend();
    println!("Installing lanxfer {latest_version}…");

    let update_result = run_update_with_retry(installed);

    // ── Phase 3: TUI — show result ───────────────────────────────────
    match update_result {
        Ok(version) => {
            screen.render(
                "Update",
                &format!("Installed lanxfer {version}"),
                Tone::Success,
                &versions,
                "enter / esc  close",
            )?;
        }
        Err(e) => {
            screen.render(
                "Update",
                "Update failed",
                Tone::Error,
                &versions,
                &format!("{e}  ·  enter / esc  close"),
            )?;
        }
    }
    screen.wait_for_close()?;
    Ok(())
}

/// Attempt the download+install up to MAX_ATTEMPTS times, backing off on
/// failure to handle GitHub API rate limits (HTTP 403/429).
fn run_update_with_retry(current_version: &str) -> Result<String> {
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=MAX_ATTEMPTS {
        let result = self_update::backends::github::Update::configure()
            .repo_owner(OWNER)
            .repo_name(REPO)
            .bin_name("lanxfer")
            .target(asset_slug())
            .no_confirm(true)
            .show_output(false)
            .show_download_progress(false)
            .current_version(current_version)
            .build()
            .context("could not configure updater")?
            .update();

        match result {
            Ok(status) => return Ok(status.version().to_string()),
            Err(e) => {
                if attempt < MAX_ATTEMPTS {
                    println!(
                        "Attempt {attempt}/{MAX_ATTEMPTS} failed ({e}), retrying in {}s…",
                        RETRY_DELAY.as_secs()
                    );
                    std::thread::sleep(RETRY_DELAY);
                }
                last_err = Some(e.into());
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("update failed after {MAX_ATTEMPTS} attempts")))
}

#[cfg(test)]
mod tests {
    use super::asset_slug;

    #[test]
    fn slug_resolves_for_this_platform() {
        // Exactly one cfg arm must match, giving a non-empty slug that
        // matches an asset published by release.yml for this platform.
        assert!(!asset_slug().is_empty());
    }
}
