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
// parts of the code was shoehorned in by Opus 5.5
const API_URL: &str = "https://api.github.com/repos/duino-coin/duco-pools-rs/releases/latest";
const BUILD_TARGET: &str = env!("BUILD_TARGET");
// releases are only installed if manifest.json is signed by this key under SIGNATURE_NAMESPACE
const RELEASE_KEY: &str = include_str!("release_key.pub");
const SIGNATURE_NAMESPACE: &str = "duco-pool-rs-release";
const MANIFEST_NAME: &str = "manifest.json";
const SIGNATURE_NAME: &str = "manifest.json.sig";
// the manifest lists a handful of assets and the signature is a small PEM block
const MAX_MANIFEST_SIZE: u64 = 64 * 1024;
const MAX_SIGNATURE_SIZE: u64 = 4 * 1024;
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
    let mut parts = tag.split('.');
    // split always yields the major part, a missing minor or patch counts as 0
    let mut next = || parts.next().map_or(Ok(0), u32::from_str);
    let (major, minor, patch) = (next()?, next()?, next()?);
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
fn verify_digest(bytes: &[u8], expected_hex: &str) -> anyhow::Result<()> {
    check_digest(&Sha256::digest(bytes), expected_hex)
}

// `actual` is the SHA-256 of the downloaded bytes, `expected_hex` the one from the signed manifest
fn check_digest(actual: &[u8], expected_hex: &str) -> anyhow::Result<()> {
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

async fn stage_download(
    mut response: reqwest::Response,
    staging: &Path,
    expected_sha256: &str,
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
    check_digest(&hasher.finalize(), expected_sha256)?;
    file.flush().await?;
    file.sync_all().await?;
    Ok(())
}

async fn get_asset(client: &reqwest::Client, url: &str) -> anyhow::Result<reqwest::Response> {
    Ok(client
        .get(url)
        .header("Accept", "application/octet-stream")
        .header("User-Agent", "duco-pool-rs")
        .send()
        .await?
        .error_for_status()?)
}

// reads a small asset (manifest, signature) into memory, at most `max` bytes
async fn download_small(mut response: reqwest::Response, max: u64) -> anyhow::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if (bytes.len() + chunk.len()) as u64 > max {
            bail!("Downloaded asset exceeds {max} bytes");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

// the signature must come from `release_key` and be made for SIGNATURE_NAMESPACE
fn verify_manifest(
    release_key: &str,
    manifest: &[u8],
    signature: &[u8],
) -> anyhow::Result<Manifest> {
    let release_key =
        ssh_key::PublicKey::from_openssh(release_key.trim()).context("Invalid release key")?;
    let signature = ssh_key::SshSig::from_pem(signature).context("Malformed manifest signature")?;
    release_key
        .verify(SIGNATURE_NAMESPACE, manifest, &signature)
        .context("Manifest signature does not match the release key")?;
    serde_json::from_slice(manifest).context("Malformed manifest")
}

async fn fetch_signed_manifest(
    client: &reqwest::Client,
    assets: &[Asset],
) -> anyhow::Result<Manifest> {
    let url = |name: &str| {
        assets
            .iter()
            .find(|asset| asset.name == name)
            .map(|asset| asset.url.as_str())
            .with_context(|| format!("Release has no {name}"))
    };
    let manifest = get_asset(client, url(MANIFEST_NAME)?).await?;
    let manifest = download_small(manifest, MAX_MANIFEST_SIZE).await?;
    let signature = get_asset(client, url(SIGNATURE_NAME)?).await?;
    let signature = download_small(signature, MAX_SIGNATURE_SIZE).await?;
    verify_manifest(RELEASE_KEY, &manifest, &signature)
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
    let tag_version = parse_tag(response.tag_name.trim_start_matches('v'))?;

    if tag_version > VERSION_CODE {
        // exact match, so the .sha256 sidecar or a target sharing our prefix is never picked
        let archive_name = format!(
            "{}-{BUILD_TARGET}{ARCHIVE_EXTENSION}",
            env!("CARGO_PKG_NAME")
        );
        for asset in &response.assets {
            if asset.name.eq_ignore_ascii_case(&archive_name) {
                // only the manifest is signed, the release around it (tag, assets) is not
                let manifest = fetch_signed_manifest(&client, &response.assets).await?;
                if manifest.tag != response.tag_name || parse_tag(&manifest.version)? != tag_version
                {
                    bail!(
                        "Signed manifest is for {}, not {}",
                        manifest.tag,
                        response.tag_name
                    );
                }
                let entry = manifest
                    .assets
                    .iter()
                    .find(|entry| entry.asset == asset.name)
                    .with_context(|| format!("Signed manifest does not list {}", asset.name))?;

                let download = get_asset(&client, &asset.url).await?;

                let executable = std::env::current_exe()?;
                let path = update_path(&executable)?;

                // the archive is streamed to disk (never in memory) and verified, then only the
                // executable is extracted into a staging file that is moved to `path` once
                // complete, so a partial or bad download is never installable
                let archive = with_suffix(&path, ARCHIVE_EXTENSION);

                let staged = stage_download(download, &archive, &entry.archive_sha256).await;
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
}

#[derive(Deserialize)]
struct Root {
    pub tag_name: String,
    pub assets: Vec<Asset>,
}

// manifest.json as written by release-verify.sh; only the fields the updater reads
#[derive(Deserialize)]
struct Manifest {
    pub version: String,
    pub tag: String,
    pub assets: Vec<ManifestAsset>,
}

#[derive(Deserialize)]
struct ManifestAsset {
    pub asset: String,
    pub archive_sha256: String,
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

    const ABC_DIGEST: &str = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

    #[test]
    fn digest_accepts_known_sha256_and_uppercase_hex() {
        verify_digest(b"abc", ABC_DIGEST).unwrap();
        verify_digest(b"abc", &ABC_DIGEST.to_uppercase()).unwrap();
    }

    #[test]
    fn digest_hashes_raw_binary_bytes() {
        verify_digest(
            b"\x00\xff\x80duco\x00",
            "8debd935adad84ee40200a35769f16d1c55598b955a6787103559eb4d71a769e",
        )
        .unwrap();
    }

    #[test]
    fn digest_rejects_changed_content() {
        let err = verify_digest(b"abd", ABC_DIGEST).unwrap_err();
        assert!(err.to_string().contains("digest mismatch"));
    }

    // throwaway ed25519 key, only used by these tests
    const TEST_KEY: &str =
        "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOoSP3h4oXDdRdeIDoHLuJKB0WdKMbM+BRXfK2CKgZuP test";
    const TEST_MANIFEST: &str = r#"{
  "version": "0.1.9",
  "tag": "v0.1.9",
  "commit": "0000000000000000000000000000000000000000",
  "assets": [
    {"target": "x86_64-unknown-linux-gnu", "asset": "duco-pool-rs-Linux-gnu-x86_64.tar.gz", "archive_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "binary_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}
  ]
}
"#;
    // ssh-keygen -Y sign -f test_key -n duco-pool-rs-release manifest.json
    const TEST_SIGNATURE: &str = "-----BEGIN SSH SIGNATURE-----
U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAg6hI/eHihcN1F14gOgcu4koHRZ0
oxsz4FFd8rYIqBm48AAAAUZHVjby1wb29sLXJzLXJlbGVhc2UAAAAAAAAABnNoYTUxMgAA
AFMAAAALc3NoLWVkMjU1MTkAAABAQQ0zQySNHWEKunSbd2BybTpylV1m3Dc4sft4J8bnmC
VTPoTxMExrI0zpI8AznIvIjsNgScraewcntHRBZ4+8Cg==
-----END SSH SIGNATURE-----
";
    // the same manifest signed by the same key, but under the namespace "other-namespace"
    const OTHER_NAMESPACE_SIGNATURE: &str = "-----BEGIN SSH SIGNATURE-----
U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAg6hI/eHihcN1F14gOgcu4koHRZ0
oxsz4FFd8rYIqBm48AAAAPb3RoZXItbmFtZXNwYWNlAAAAAAAAAAZzaGE1MTIAAABTAAAA
C3NzaC1lZDI1NTE5AAAAQItj0hckXiD5jsp3KG6v/nUl03h2ckAINdNpzgECprhx4mlUQa
4HWv7kESyS3xQu5In7Q3SaU6Gui9UdtZEl/Ag=
-----END SSH SIGNATURE-----
";

    #[test]
    fn release_key_is_a_security_key() {
        let key = ssh_key::PublicKey::from_openssh(RELEASE_KEY.trim()).unwrap();
        assert_eq!(key.algorithm(), ssh_key::Algorithm::SkEd25519);
    }

    #[test]
    fn manifest_accepts_valid_signature() {
        let manifest = verify_manifest(
            TEST_KEY,
            TEST_MANIFEST.as_bytes(),
            TEST_SIGNATURE.as_bytes(),
        )
        .unwrap();
        assert_eq!(manifest.tag, "v0.1.9");
        assert_eq!(manifest.assets[0].archive_sha256, "a".repeat(64));
    }

    #[test]
    fn manifest_rejects_changed_content() {
        let changed = TEST_MANIFEST.replace("aaaa", "cccc");
        assert!(verify_manifest(TEST_KEY, changed.as_bytes(), TEST_SIGNATURE.as_bytes()).is_err());
    }

    #[test]
    fn manifest_rejects_other_namespace() {
        let result = verify_manifest(
            TEST_KEY,
            TEST_MANIFEST.as_bytes(),
            OTHER_NAMESPACE_SIGNATURE.as_bytes(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn manifest_rejects_other_key() {
        let result = verify_manifest(
            RELEASE_KEY,
            TEST_MANIFEST.as_bytes(),
            TEST_SIGNATURE.as_bytes(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn digest_rejects_malformed_values() {
        for digest in [
            String::new(),
            "a".repeat(63),
            "a".repeat(65),
            "g".repeat(64),
            format!("sha256:{ABC_DIGEST}"),
            format!("{ABC_DIGEST}:extra"),
            format!("{ABC_DIGEST} "),
        ] {
            assert!(
                verify_digest(b"abc", &digest).is_err(),
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
