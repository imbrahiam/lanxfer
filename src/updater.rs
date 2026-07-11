use anyhow::{Context, Result};

use crate::picker::{StatusScreen, Tone};

const OWNER: &str = "imbrahiam";
const REPO: &str = "lanxfer";

pub fn run(check_only: bool, assume_yes: bool) -> Result<()> {
    let installed = env!("CARGO_PKG_VERSION");
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

    screen.render(
        "Update",
        &format!("Installing {latest_version}…"),
        Tone::Info,
        &versions,
        "Downloading and replacing the current executable",
    )?;
    // The terminal is in raw mode under ratatui: self_update must never
    // print (garbles the alternate screen) nor prompt on stdin (blocks
    // forever — line input doesn't work in raw mode). We already confirmed
    // in our own UI above.
    let status = self_update::backends::github::Update::configure()
        .repo_owner(OWNER)
        .repo_name(REPO)
        .bin_name("lanxfer")
        .no_confirm(true)
        .show_output(false)
        .show_download_progress(false)
        .current_version(installed)
        .build()
        .context("could not configure updater")?
        .update()
        .context("update failed")?;
    screen.render(
        "Update",
        &format!("Installed lanxfer {}", status.version()),
        Tone::Success,
        &versions,
        "enter / esc  close",
    )?;
    screen.wait_for_close()?;
    Ok(())
}
