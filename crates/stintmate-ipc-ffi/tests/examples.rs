//! Builds and runs `examples/c` (MSVC) and `examples/csharp` (csc.exe or the dotnet SDK)
//! against an echo endpoint served from Rust. A toolchain that is not installed skips its
//! test with a reason printed on stderr.

use std::path::{Path, PathBuf};
use std::process::Command;

use stintmate_ipc::MethodOpts;
use stintmate_ipc::blocking::Endpoint;
use stintmate_ipc::handshake::{PeerInfo, new_nonce};
use stintmate_ipc::keyfile::{KeyFile, KeySource, ensure_key};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..")
}

/// `target/<profile>` of this test run.
fn profile_dir() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    exe.parent().unwrap().parent().unwrap().to_path_buf()
}

struct Echo {
    ep: Endpoint,
    pipe: String,
    key: PathBuf,
    work: PathBuf,
}

impl Echo {
    fn start(tag: &str) -> Echo {
        let n: String = new_nonce()[..4]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let pipe = format!("stintmate-ffi-ex-{tag}-{n}");
        let work = std::env::temp_dir().join(format!("stintmate-ffi-ex-{tag}-{n}"));
        std::fs::create_dir_all(&work).unwrap();
        let key = work.join("key");
        ensure_key(&key).unwrap();
        let ep = Endpoint::builder(
            PeerInfo::new("server"),
            KeySource::File(KeyFile::open(&key)),
        )
        .pipe(&pipe)
        .build()
        .unwrap();
        ep.method("t.echo", MethodOpts::default(), |_ctx, params| Ok(params))
            .unwrap();
        ep.start().unwrap();
        Echo {
            ep,
            pipe,
            key,
            work,
        }
    }
}

impl Drop for Echo {
    fn drop(&mut self) {
        self.ep.stop();
        let _ = std::fs::remove_dir_all(&self.work);
    }
}

fn skip(reason: &str) {
    // CI sets STINTMATE_REQUIRE_EXAMPLES: a runner without the toolchain fails instead.
    if std::env::var_os("STINTMATE_REQUIRE_EXAMPLES").is_some_and(|v| !v.is_empty()) {
        panic!("example required but not built: {reason}");
    }
    eprintln!("SKIPPED: {reason}");
}

fn run_example(exe: &Path, echo: &Echo, dll_dir: &Path) {
    let path = format!(
        "{};{}",
        dll_dir.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let out = Command::new(exe)
        .arg(format!("\\\\.\\pipe\\{}", echo.pipe))
        .arg(&echo.key)
        .env("PATH", path)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.status.success() && text.contains("echo ok"),
        "example failed: {text}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    eprintln!(
        "RAN {}: {}",
        exe.display(),
        text.trim().replace('\n', " | ")
    );
}

fn vcvars() -> Option<PathBuf> {
    let vswhere =
        Path::new(r"C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe");
    let out = Command::new(vswhere)
        .args([
            "-latest",
            "-products",
            "*",
            "-requires",
            "Microsoft.VisualStudio.Component.VC.Tools.x86.x64",
            "-property",
            "installationPath",
        ])
        .output()
        .ok()?;
    let dir = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    let bat = Path::new(&dir).join(r"VC\Auxiliary\Build\vcvars64.bat");
    bat.exists().then_some(bat)
}

fn library_ready(dir: &Path) -> bool {
    dir.join("stintmate_ipc.dll").exists() && dir.join("stintmate_ipc.dll.lib").exists()
}

#[test]
fn c_example_echoes() {
    let dir = profile_dir();
    if !library_ready(&dir) {
        return skip("target/<profile>/stintmate_ipc.dll is not built (run cargo build first)");
    }
    let Some(vcvars) = vcvars() else {
        return skip("no MSVC toolchain found through vswhere");
    };
    let echo = Echo::start("c");
    let out = echo.work.join("c");
    std::fs::create_dir_all(&out).unwrap();
    let r = root();
    let bat = out.join("build.bat");
    std::fs::write(
        &bat,
        format!(
            "@echo off\r\ncall \"{}\" >nul || exit /b 1\r\ncl /nologo /W3 /I \"{}\" \"{}\" /Fo\"{}\\\\\" /Fe\"{}\" /link \"{}\"\r\n",
            vcvars.display(),
            r.join("crates/stintmate-ipc-ffi/include").display(),
            r.join("examples/c/echo.c").display(),
            out.display(),
            out.join("echo.exe").display(),
            dir.join("stintmate_ipc.dll.lib").display(),
        ),
    )
    .unwrap();
    let built = Command::new("cmd").arg("/C").arg(&bat).output().unwrap();
    assert!(
        built.status.success(),
        "cl failed: {}{}",
        String::from_utf8_lossy(&built.stdout),
        String::from_utf8_lossy(&built.stderr)
    );
    run_example(&out.join("echo.exe"), &echo, &dir);
}

#[test]
fn csharp_example_echoes() {
    let dir = profile_dir();
    if !library_ready(&dir) {
        return skip("target/<profile>/stintmate_ipc.dll is not built (run cargo build first)");
    }
    let csc = Path::new(r"C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe");
    let dotnet_sdk = Command::new("dotnet")
        .arg("--list-sdks")
        .output()
        .map(|o| !o.stdout.is_empty())
        .unwrap_or(false);
    if !csc.exists() && !dotnet_sdk {
        return skip("neither csc.exe (.NET Framework 4) nor a dotnet SDK was found");
    }
    let echo = Echo::start("cs");
    let out = echo.work.join("cs");
    std::fs::create_dir_all(&out).unwrap();
    let src = root().join("examples/csharp");
    let exe = if dotnet_sdk {
        let built = Command::new("dotnet")
            .args(["build", "-c", "Release", "-o"])
            .arg(&out)
            .arg(src.join("Echo.csproj"))
            .output()
            .unwrap();
        assert!(
            built.status.success(),
            "dotnet build failed: {}",
            String::from_utf8_lossy(&built.stdout)
        );
        out.join("Echo.exe")
    } else {
        let exe = out.join("Echo.exe");
        let built = Command::new(csc)
            .args(["/nologo", "/platform:x64", "/target:exe"])
            .arg(format!("/out:{}", exe.display()))
            .arg(src.join("Echo.cs"))
            .arg(src.join("Native.cs"))
            .output()
            .unwrap();
        assert!(
            built.status.success(),
            "csc failed: {}",
            String::from_utf8_lossy(&built.stdout)
        );
        exe
    };
    run_example(&exe, &echo, &dir);
}
