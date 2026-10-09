//! atomic_write.rs

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
};

/// Writes a temporary file next to `path`, syncs it and renames it over `path`
pub(crate) fn write_file_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    let tmp = tmp_path(path);

    let written = fs::File::create(&tmp).and_then(|mut file| {
        file.write_all(data)?;
        file.sync_all()
    });

    if let Err(e) = written.and_then(|_| fs::rename(&tmp, path)) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }

    if let Err(e) = sync_parent_dir(path) {
        log::warn!("Cannot sync the directory of {}: {}", path.display(), e);
    }

    Ok(())
}

pub(crate) fn tmp_path(path: &Path) -> PathBuf {
    path.with_added_extension("tmp")
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };

    fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> io::Result<()> {
    Ok(())
}
