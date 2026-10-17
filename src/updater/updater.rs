#![allow(dead_code)]
use anyhow::{bail, Context};
#[cfg(not(target_os = "windows"))]
use flate2::read::GzDecoder;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::str::FromStr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
 const API_URL: &str = "https://api.github.com/repos/duino-coin/duco-pools-rs/releases/latest";
const BUILD_TARGET: &str = env!("BUILD_TARGET");
// release archive as packaged by actions-rust-release
#[cfg(not(target_os = "windows"))]
const ARCHIVE_EXTENSION: &str = ".tar.gz";
#[cfg(target_os = "windows")]
const ARCHIVE_EXTENSION: &str = ".zip";
// release builds are ~8 MiB; anything far beyond that is not our executable
const MAX_EXECUTABLE_SIZE: u64 = 64 * 1024 * 1024;
const fn parse_version_component(value: &str) -> u32 {
    match u32::from_str_radix(value, 10) {
        Ok(number) => number,
        Err(_) => panic!("invalid Cargo version component"),
    }
}
pub const VERSION_MAJOR: u32 = parse_version_component(env!("CARGO_PKG_VERSION_MAJOR"));

pub const VERSION_MINOR: u32 = parse_version_component(env!("CARGO_PKG_VERSION_MINOR"));

pub const VERSION_PATCH: u32 = parse_version_component(env!("CARGO_PKG_VERSION_PATCH"));

const _: () = assert!(
    VERSION_MINOR < 100 && VERSION_PATCH < 100,
    "minor and patch must be below 100"
);

const fn create_version_code(major: u32, minor: u32, patch: u32) -> u32 {
    major * 10_000 + minor * 100 + patch
}

pub const VERSION_CODE: u32 = create_version_code(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);
// VERSION_MAJOR * 10_000 + VERSION_MINOR * 100 + VERSION_PATCH;

fn parse_tag(tag: &str) -> anyhow::Result<u32> {
    let tag_split = tag.split('.').collect::<Vec<&str>>();
    let major = if !tag_split.is_empty() {
        u32::from_str(tag_split[0])?
    } else {
        0
    };
    let minor = if tag_split.len() >= 2 {
        u32::from_str(tag_split[1])?
    } else {
        0
    };
    let patch = if tag_split.len() >= 3 {
        u32::from_str(tag_split[2])?
    } else {
        0
    };
    // create_version_code packs minor and patch into two digits each and must not overflow
    if minor >= 100 || patch >= 100 || major > u32::MAX / 10_000 - 1 {
        bail!("Unsupported release tag {tag}");
    }

    Ok(create_version_code(major, minor, patch))
}

pub struct DownloadedUpdate {
    pub version: String,
    executable: PathBuf,
    path: PathBuf,
}

pub struct RestartHelper {
    child: Option<Child>,
}

impl Drop for RestartHelper {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}

impl RestartHelper {
    pub fn detach(mut self) {
        let mut child = self.child.take().unwrap();
        // catch early failures while the pool is alive (no way of saving at this point tho..)
        tokio::spawn(async move {
            match child.wait().await {
                Ok(status) if status.success() => {}
                Ok(status) => {
                    log::error!("Update helper exited with {status}; see update-restart.log")
                }
                Err(err) => log::error!("Failed to wait for update helper: {err}"),
            }
        });
    }
}

fn update_path(executable: &Path) -> anyhow::Result<PathBuf> {
    let mut name = executable
        .file_stem()
        .context("Executable has no filename")?
        .to_os_string();
    name.push("-update");
    if let Some(extension) = executable.extension() {
        name.push(".");
        name.push(extension);
    }
    Ok(executable.with_file_name(name))
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut path = path.as_os_str().to_os_string();
    path.push(suffix);
    PathBuf::from(path)
}

fn restart_script(configured: &str, working_dir: &Path) -> anyhow::Result<PathBuf> {
    let script = Path::new(configured);

    let path = working_dir.join(script);
    let metadata = std::fs::metadata(&path)
        .with_context(|| format!("Cannot find update script {}", path.display()))?;
    if !metadata.is_file() {
        bail!("Update script is not a file: {}", path.display());
    }

    Ok(path)
}

pub async fn spawn_update_restart(
    update: &DownloadedUpdate,
    configured_script: &str,
) -> anyhow::Result<RestartHelper> {
    spawn_update_restart_in(update, configured_script, &std::env::current_dir()?).await
}

async fn spawn_update_restart_in(
    update: &DownloadedUpdate,
    configured_script: &str,
    working_dir: &Path,
) -> anyhow::Result<RestartHelper> {
    let script = restart_script(configured_script, working_dir)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(working_dir.join("update-restart.log"))?;

    #[cfg(windows)]
    let mut command = {
        let mut command = Command::new("powershell.exe");
        command
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "RemoteSigned",
                "-File",
            ])
            .arg(&script);
        command.creation_flags(0x08000000); // CREATE_NO_WINDOW
        command
    };
    #[cfg(unix)]
    let mut command = {
        let mut command = Command::new("bash");
        command.arg(&script).process_group(0);
        command
    };

    let child = command
        .current_dir(working_dir)
        .env("UPDATE_PARENT_PID", std::process::id().to_string())
        .env("UPDATE_SOURCE", &update.path)
        .env("UPDATE_EXECUTABLE", &update.executable)
        .env("UPDATE_WORKING_DIR", working_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log))
        .kill_on_drop(false)
        .spawn()
        .context("Failed to launch update helper")?;

    let mut helper = RestartHelper { child: Some(child) };
    let child = helper.child.as_mut().unwrap();
    let stdout = child.stdout.take().context("Update helper has no stdout")?;
    let mut reader = BufReader::new(stdout.take(128));
    let mut ready = String::new();
    tokio::time::timeout(Duration::from_secs(15), reader.read_line(&mut ready))
        .await
        .context("Update helper readiness timed out; see update-restart.log")??;
    if ready.trim_end() != "UPDATE_READY" {
        bail!("Update helper did not acknowledge readiness ({ready:?}); see update-restart.log");
    }
    if let Some(status) = child.try_wait()? {
        bail!("Update helper exited before shutdown with {status}; see update-restart.log");
    }
    Ok(helper)
}

#[cfg(test)]
fn verify_digest(bytes: &[u8], digest: Option<&str>) -> anyhow::Result<()> {
    check_digest(&Sha256::digest(bytes), digest)
}

// `actual` is the SHA-256 of the downloaded bytes
fn check_digest(actual: &[u8], digest: Option<&str>) -> anyhow::Result<()> {
    let digest = digest.context("Release asset has no digest")?;
    let (algorithm, expected_hex) = digest
        .split_once(':')
        .context("Malformed asset digest: expected sha256:<hex>")?;
    if algorithm != "sha256" {
        bail!("Unsupported asset digest algorithm: {algorithm}");
    }

    let mut expected = [0u8; 32];
    hex::decode_to_slice(expected_hex, &mut expected)
        .context("Invalid SHA-256 digest: expected 64 hexadecimal characters")?;
    if actual[..] != expected {
        bail!(
            "Update SHA-256 digest mismatch: expected {expected_hex}, calculated {}",
            hex::encode(actual)
        );
    }
    Ok(())
}

// writes the body to `staging` while hashing it; same checks and order as before
// (empty download, then digest)
async fn stage_download(
    mut response: reqwest::Response,
    staging: &Path,
    digest: Option<&str>,
) -> anyhow::Result<()> {
    let mut file = tokio::fs::File::create(staging).await?;
    let mut hasher = Sha256::new();
    let mut len = 0u64;
    while let Some(chunk) = response.chunk().await? {
        len += chunk.len() as u64;
        // the archive is compressed, so it's never larger than the executable it holds
        if len > MAX_EXECUTABLE_SIZE {
            bail!("Downloaded update exceeds {MAX_EXECUTABLE_SIZE} bytes");
        }
        hasher.update(&chunk);
        file.write_all(&chunk).await?;
    }
    if len == 0 {
        bail!("Downloaded update is empty");
    }
    check_digest(&hasher.finalize(), digest)?;
    file.flush().await?;
    file.sync_all().await?;
    Ok(())
}

// pulls only the executable out of the verified release archive into `staging`
#[cfg(target_os = "windows")]
fn stage_executable(archive: &Path, staging: &Path) -> anyhow::Result<()> {
    use std::io::Read;
    let member = format!("{}{}", env!("CARGO_PKG_NAME"), std::env::consts::EXE_SUFFIX);
    let mut archive = zip::ZipArchive::new(std::fs::File::open(archive)?)?;
    let entry = match archive.by_name(&member) {
        Ok(entry) => entry,
        Err(zip::result::ZipError::FileNotFound) => bail!("{member} not found in the archive"),
        Err(err) => return Err(err.into()),
    };
    if !entry.is_file() {
        bail!("{member} in the archive is not a regular file");
    }
    let len = entry.size();
    if len == 0 {
        bail!("{member} in the archive is empty");
    }
    if len > MAX_EXECUTABLE_SIZE {
        bail!("{member} in the archive exceeds {MAX_EXECUTABLE_SIZE} bytes");
    }
    let mut file = std::fs::File::create(staging)?;
    // the declared size is untrusted, so the decompressed stream is bounded as well
    let copied = std::io::copy(&mut entry.take(len), &mut file)?;
    if copied != len {
        bail!("{member} in the archive is truncated");
    }
    file.sync_all()?;

    Ok(())
}

// pulls only the executable out of the verified release archive into `staging`
#[cfg(not(target_os = "windows"))]
fn stage_executable(archive: &Path, staging: &Path) -> anyhow::Result<()> {
    let member = format!("{}{}", env!("CARGO_PKG_NAME"), std::env::consts::EXE_SUFFIX);
    let mut archive = tar::Archive::new(GzDecoder::new(std::fs::File::open(archive)?));
    for entry in archive.entries()? {
        let mut entry = entry?;
        if entry.path()? != Path::new(&member) {
            continue;
        }
        if !entry.header().entry_type().is_file() {
            bail!("{member} in the archive is not a regular file");
        }
        // the entry reader stops at the header size, so this bounds what is written
        let len = entry.header().size()?;
        if len == 0 {
            bail!("{member} in the archive is empty");
        }
        if len > MAX_EXECUTABLE_SIZE {
            bail!("{member} in the archive exceeds {MAX_EXECUTABLE_SIZE} bytes");
        }
        let mut file = std::fs::File::create(staging)?;
        std::io::copy(&mut entry, &mut file)?;
        file.sync_all()?;
        return Ok(());
    }
    bail!("{member} not found in the archive")
}

// `timeout` bounds connecting and each read rather than the whole request, so a slow but
// progressing download of the release archive is not cut off
pub async fn fetch_latest_version(timeout: Duration) -> anyhow::Result<Option<DownloadedUpdate>> {
    let client = reqwest::Client::builder()
        .connect_timeout(timeout)
        .read_timeout(timeout)
        .build()?;
    let response = client
        .get(API_URL)
        .header(
            "User-Agent",
            concat!("duco-pool-rs/", env!("CARGO_PKG_VERSION")),
        )
        .send()
        .await?
        .error_for_status()?
        .json::<Root>()
        .await?;
    let tag_version = parse_tag(&response.tag_name.replace("v", ""))?;

    if tag_version > VERSION_CODE {
        // exact match, so the .sha256 sidecar or a target sharing our prefix is never picked
        let archive_name = format!(
            "{}-{BUILD_TARGET}{ARCHIVE_EXTENSION}",
            env!("CARGO_PKG_NAME")
        );
        for asset in response.assets {
            if asset.name.eq_ignore_ascii_case(&archive_name) {
                let download = client
                    .get(asset.url)
                    .header(
                        "Accept",
                        "application/octet-stream"
                    )
                    .header(
                        "User-Agent",
                        "duco-pool-rs"
                    )
                    .send()
                    .await?
                    .error_for_status()?;

                let executable = std::env::current_exe()?;
                let path = update_path(&executable)?;

                // the archive is streamed to disk (never in memory) and verified, then only the
                // executable is extracted into a staging file that is moved to `path` once
                // complete, so a partial or bad download is never installable
                let archive = with_suffix(&path, ARCHIVE_EXTENSION);

                let staged = stage_download(download, &archive, asset.digest.as_deref()).await;
                if let Err(err) = staged {
                    let _ = tokio::fs::remove_file(&archive).await;
                    return Err(err).with_context(|| {
                        format!("Downloading or verifying asset {} failed", asset.name)
                    });
                }
                let staging = with_suffix(&path, ".part");

                let extracted = tokio::task::spawn_blocking({
                    let (archive, staging) = (archive.clone(), staging.clone());
                    move || stage_executable(&archive, &staging)
                })
                .await;
                let _ = tokio::fs::remove_file(&archive).await;
                if let Err(err) = extracted.map_err(anyhow::Error::from).and_then(|res| res) {
                    let _ = tokio::fs::remove_file(&staging).await;
                    return Err(err)
                        .with_context(|| format!("Extracting asset {} failed", asset.name));
                }
                tokio::fs::rename(&staging, &path).await?;
                return Ok(Some(DownloadedUpdate {
                    version: response.tag_name,
                    executable,
                    path,
                }));
            }
        }
    }
    Ok(None)
}

// only the fields the updater reads; GitHub sends several of the others as null
// (e.g. a release without a title), which would fail the whole update check
#[derive(Deserialize)]
struct Asset {
    pub url: String,
    pub name: String,
    pub digest: Option<String>,
}

#[derive(Deserialize)]
struct Root {
    pub tag_name: String,
    pub assets: Vec<Asset>,
}

// written by GPT-6
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    const TEST_SCRIPT: &str = if cfg!(windows) {
        "config/update-restart.ps1"
    } else {
        "config/update-restart.sh"
    };

    const ABC_DIGEST: &str =
        "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn digest_accepts_known_sha256_and_uppercase_hex() {
        verify_digest(b"abc", Some(ABC_DIGEST)).unwrap();
        let uppercase = format!(
            "sha256:{}",
            ABC_DIGEST.strip_prefix("sha256:").unwrap().to_uppercase()
        );
        verify_digest(b"abc", Some(&uppercase)).unwrap();
    }

    #[test]
    fn digest_hashes_raw_binary_bytes() {
        verify_digest(
            b"\x00\xff\x80duco\x00",
            Some("sha256:8debd935adad84ee40200a35769f16d1c55598b955a6787103559eb4d71a769e"),
        )
        .unwrap();
    }

    #[test]
    fn digest_rejects_changed_content() {
        let err = verify_digest(b"abd", Some(ABC_DIGEST)).unwrap_err();
        assert!(err.to_string().contains("digest mismatch"));
    }

    #[test]
    fn digest_rejects_missing_digest() {
        let err = verify_digest(b"abc", None).unwrap_err();
        assert!(err.to_string().contains("no digest"));
    }

    #[test]
    fn digest_rejects_unsupported_algorithm() {
        let digest = ABC_DIGEST.replacen("sha256", "sha512", 1);
        let err = verify_digest(b"abc", Some(&digest)).unwrap_err();
        assert!(err
            .to_string()
            .contains("Unsupported asset digest algorithm"));
    }

    #[test]
    fn digest_rejects_malformed_values() {
        for digest in [
            String::new(),
            "sha256".into(),
            "sha256:".into(),
            format!("sha256:{}", "a".repeat(63)),
            format!("sha256:{}", "a".repeat(65)),
            format!("sha256:{}", "g".repeat(64)),
            format!("{ABC_DIGEST}:extra"),
            format!("{ABC_DIGEST} "),
        ] {
            assert!(
                verify_digest(b"abc", Some(&digest)).is_err(),
                "Accepted {digest:?}"
            );
        }
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let id = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "duco updater {} {id} {}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    // the release zip is deflate-compressed, as produced by Compress-Archive
    #[cfg(windows)]
    #[test]
    fn stage_executable_extracts_from_zip() {
        let dir = TestDirectory::new();
        let member = format!("{}.exe", env!("CARGO_PKG_NAME"));
        let contents = b"MZ duco".repeat(4096);
        fs::write(dir.0.join(&member), &contents).unwrap();
        fs::write(dir.0.join("README.md"), b"readme").unwrap();
        let archive = dir.0.join("release.zip");
        let status = std::process::Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command"])
            .arg(format!(
                "Compress-Archive -Path '{}','{}' -DestinationPath '{}'",
                dir.0.join(&member).display(),
                dir.0.join("README.md").display(),
                archive.display()
            ))
            .status()
            .unwrap();
        assert!(status.success());

        let staging = dir.0.join("staged.part");
        stage_executable(&archive, &staging).unwrap();
        assert_eq!(fs::read(&staging).unwrap(), contents);

        fs::remove_file(dir.0.join(&member)).unwrap();
        let without = dir.0.join("without.zip");
        let status = std::process::Command::new("powershell")
            .args(["-NoProfile", "-NonInteractive", "-Command"])
            .arg(format!(
                "Compress-Archive -Path '{}' -DestinationPath '{}'",
                dir.0.join("README.md").display(),
                without.display()
            ))
            .status()
            .unwrap();
        assert!(status.success());
        let err = stage_executable(&without, &staging).unwrap_err();
        assert!(err.to_string().contains("not found in the archive"));
    }

    fn fixture() -> (TestDirectory, DownloadedUpdate) {
        let dir = TestDirectory::new();
        fs::create_dir(dir.0.join("config")).unwrap();
        let script = if cfg!(windows) {
            "update-restart.ps1"
        } else {
            "update-restart.sh"
        };
        fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("config")
                .join(script),
            dir.0.join("config").join(script),
        )
        .unwrap();
        let executable = dir.0.join(if cfg!(windows) { "pool.exe" } else { "pool" });
        let path = update_path(&executable).unwrap();
        fs::write(&executable, b"old executable").unwrap();
        fs::write(&path, b"new executable").unwrap();
        (
            dir,
            DownloadedUpdate {
                version: "test".into(),
                executable,
                path,
            },
        )
    }

    #[tokio::test]
    async fn bundled_helper_waits_without_replacing_or_stopping_parent() {
        let (dir, update) = fixture();
        let mut helper = spawn_update_restart_in(&update, TEST_SCRIPT, &dir.0)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "{err:#}: {}",
                    String::from_utf8_lossy(
                        &fs::read(dir.0.join("update-restart.log")).unwrap_or_default()
                    )
                );
            });
        let child = helper.child.as_mut().unwrap();
        assert!(child.try_wait().unwrap().is_none());
        assert_eq!(fs::read(&update.executable).unwrap(), b"old executable");
        assert_eq!(fs::read(&update.path).unwrap(), b"new executable");
        // The test process is the parent; explicitly reap the helper before cleanup.
        child.kill().await.unwrap();
    }

    #[tokio::test]
    async fn missing_download_never_acknowledges_readiness() {
        let (dir, update) = fixture();
        fs::remove_file(&update.path).unwrap();
        let result = spawn_update_restart_in(&update, TEST_SCRIPT, &dir.0).await;
        assert!(result.is_err());
        assert_eq!(fs::read(&update.executable).unwrap(), b"old executable");
    }

    #[tokio::test]
    async fn missing_script_leaves_executable_untouched() {
        let (dir, update) = fixture();
        let result = spawn_update_restart_in(&update, "missing-script", &dir.0).await;
        assert!(result.is_err());
        assert_eq!(fs::read(&update.executable).unwrap(), b"old executable");
    }
}
