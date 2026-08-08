//! Small filesystem helpers for durable file replacement.

use std::io;
use std::path::Path;

/// Replace `destination` with `temporary` across supported host platforms.
///
/// Unix `rename` atomically replaces a regular file. Windows requires the
/// existing destination to be removed first; callers already use a temporary
/// file, so a crash can leave the old destination or the temporary artifact,
/// but never a partially written destination.
pub fn replace_file(temporary: impl AsRef<Path>, destination: impl AsRef<Path>) -> io::Result<()> {
    let temporary = temporary.as_ref();
    let destination = destination.as_ref();
    #[cfg(windows)]
    if destination.exists() {
        std::fs::remove_file(destination)?;
    }
    std::fs::rename(temporary, destination)
}
