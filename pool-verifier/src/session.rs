//! Process-wide run infrastructure shared by functional runs and benchmarks.
//!
//! Both entry points need the same three things before doing any work: the
//! verifier/repository directories, exclusive ownership of the shared `work/`
//! cache, and a Ctrl-C flag that long-running steps can poll.

use anyhow::{Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

/// Directories every run works with.
pub struct Paths {
    /// The verifier crate (`pool-verifier/`).
    pub home: PathBuf,
    /// The pool repository that contains the verifier.
    pub root: PathBuf,
    /// Disposable source copies, build caches, and the run lock (`pool-verifier/work`).
    pub work: PathBuf,
}

impl Paths {
    /// Resolves the directories from the verifier's own manifest location and
    /// makes sure `work/` exists.
    pub fn discover() -> Result<Self> {
        let home = PathBuf::from(env!("CARGO_MANIFEST_DIR")).canonicalize()?;
        let root = home
            .parent()
            .context("Verifier must live inside the pool repository")?
            .to_path_buf();
        let work = home.join("work");
        fs::create_dir_all(&work)?;
        Ok(Self { home, root, work })
    }
}

/// Exclusive ownership of `work/run.lock`; the file is removed on drop.
///
/// Runs share a disposable source tree and Cargo cache. Serializing ownership
/// prevents one run from rebuilding or mirroring while another launches cases.
pub struct RunLock(PathBuf);

impl RunLock {
    /// Creates the lock file, failing if another run already holds it.
    pub fn acquire(work: &Path) -> Result<Self> {
        let path = work.join("run.lock");
        // `create_new` is the atomic "only if absent" check. The handle is
        // dropped immediately so Windows lets the guard delete the file later.
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .with_context(|| {
                format!(
                    "Another verifier run may be active. If a previous run crashed, remove {} after checking that it has stopped",
                    path.display()
                )
            })?;
        Ok(Self(path))
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        // Best-effort cleanup is appropriate in Drop: a stale lock is safer than
        // panicking while another error or Ctrl-C is already unwinding the run.
        let _ = fs::remove_file(&self.0);
    }
}

/// Returns a flag that becomes `true` once Ctrl-C is pressed.
///
/// Nothing is killed here; every long-running step polls the flag and shuts
/// down its own child processes so artifacts and cleanup stay consistent.
pub fn interrupt_flag() -> Arc<AtomicBool> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let flag = cancelled.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            flag.store(true, Ordering::SeqCst);
            eprintln!("\nInterrupt received; stopping owned test processes.");
        }
    });
    cancelled
}

/// Completes once `cancelled` is set. Intended for use in `tokio::select!`.
pub async fn interrupted(cancelled: &AtomicBool) {
    while !cancelled.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
