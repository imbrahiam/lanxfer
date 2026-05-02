use anyhow::{Result, anyhow, bail};
use std::path::{Component, Path, PathBuf};
use sysinfo::Disks;

use crate::protocol::DestinationInfo;

pub fn list_destinations() -> Vec<DestinationInfo> {
    let mut destinations = Vec::new();
    let disks = Disks::new_with_refreshed_list();

    for disk in disks.list() {
        let mount = disk.mount_point();
        let path = mount.to_string_lossy().to_string();
        let name = disk.name().to_string_lossy();
        destinations.push(DestinationInfo {
            label: format!("{name} ({path})"),
            path,
            available_bytes: disk.available_space(),
            read_only: disk.is_read_only(),
        });
    }

    destinations.sort_by(|a, b| a.path.cmp(&b.path));
    destinations
}

pub fn sanitize_relative_path(input: &str) -> Result<PathBuf> {
    let path = Path::new(input);
    if path.is_absolute() {
        bail!("absolute paths are not allowed: {input}");
    }

    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir => bail!("path traversal is not allowed: {input}"),
            Component::RootDir | Component::Prefix(_) => {
                bail!("invalid relative path: {input}");
            }
        }
    }

    if out.as_os_str().is_empty() {
        bail!("empty relative path");
    }
    Ok(out)
}

pub fn ensure_destination_root(path: &str) -> Result<PathBuf> {
    let destination = Path::new(path);
    if !destination.exists() {
        bail!("destination path does not exist: {}", destination.display());
    }
    if !destination.is_dir() {
        bail!(
            "destination path is not a directory: {}",
            destination.display()
        );
    }
    Ok(destination.to_path_buf())
}

pub fn build_target_paths(
    destination_root: &str,
    relative_path: &str,
) -> Result<(PathBuf, PathBuf)> {
    let root = ensure_destination_root(destination_root)?;
    let relative = sanitize_relative_path(relative_path)?;
    let final_path = root.join(relative);

    let file_name = final_path
        .file_name()
        .ok_or_else(|| anyhow!("invalid target file path"))?
        .to_string_lossy()
        .to_string();
    let part_name = format!("{file_name}.lanxfer.part");
    let part_path = final_path.with_file_name(part_name);
    Ok((final_path, part_path))
}
