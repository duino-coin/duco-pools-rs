use std::env;

fn main() {
    let target_os =
        env::var("CARGO_CFG_TARGET_OS").expect("Missing target OS");
    let target_env =
        env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    let target_arch =
        env::var("CARGO_CFG_TARGET_ARCH").expect("Missing target architecture");

    // Customize these names to match your artifact naming convention.
    let os = match target_os.as_str() {
        "linux" => "Linux",
        "windows" => "Windows",
        "macos" => "macOS",
        other => other,
    };

    let arch = match target_arch.as_str() {
        "aarch64" => "arm64",
        "x86_64" => "x86_64",
        "x86" => "x86",
        other => other,
    };

    let target = if target_env.is_empty() {
        format!("{os}-{arch}")
    } else {
        format!("{os}-{target_env}-{arch}")
    }.to_lowercase();

    println!("cargo:rustc-env=BUILD_TARGET={target}");
}
