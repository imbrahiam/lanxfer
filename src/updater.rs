use anyhow::{Context, Result, bail};
use inquire::Confirm;

use crate::ui;

const OWNER: &str = "imbrahiam";
const REPO: &str = "lanxfer";

pub fn run(check_only: bool, assume_yes: bool) -> Result<()> {
    ui::banner();
    ui::section("Update");
    ui::kv("installed", env!("CARGO_PKG_VERSION"));
    ui::info("checking GitHub Releases…");

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
    ui::kv("latest", latest.version.trim_start_matches('v'));

    if !self_update::version::bump_is_greater(env!("CARGO_PKG_VERSION"), &latest.version)? {
        ui::success("already up to date");
        return Ok(());
    }
    if check_only {
        ui::warn("update available — run `lanxfer update`");
        return Ok(());
    }

    if !assume_yes
        && !Confirm::new("Download and install this update?")
            .with_default(true)
            .prompt()
            .unwrap_or(false)
    {
        bail!("update cancelled");
    }

    let status = self_update::backends::github::Update::configure()
        .repo_owner(OWNER)
        .repo_name(REPO)
        .bin_name("lanxfer")
        .show_download_progress(true)
        .current_version(env!("CARGO_PKG_VERSION"))
        .build()
        .context("could not configure updater")?
        .update()
        .context("update failed")?;
    ui::success(&format!("installed lanxfer {}", status.version()));
    Ok(())
}
