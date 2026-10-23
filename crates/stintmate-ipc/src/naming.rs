//! Endpoint names, the workspace and the endpoint registry (SPEC 4).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use serde_json::{Map, Value as Json, json};

use crate::error::NameError;
use crate::security;

/// The prefix of every pipe path.
pub const PIPE_PREFIX: &str = "\\\\.\\pipe\\";

/// Lowercases `A`-`Z` only (SPEC 4.1: identical in every language).
pub fn ascii_lower(s: &str) -> String {
    s.to_ascii_lowercase()
}

/// The `<user>` part of endpoint names: the Windows user name, ASCII-lowercased.
pub fn pipe_user() -> Result<String, NameError> {
    security::user_name()
        .map(|u| ascii_lower(&u))
        .map_err(|e| NameError::User(e.to_string()))
}

fn valid_id(s: &str, max: usize) -> bool {
    let b = s.as_bytes();
    let first = |c: u8| c.is_ascii_lowercase() || c.is_ascii_digit();
    !b.is_empty()
        && b.len() <= max
        && first(b[0])
        && b[1..].iter().all(|&c| first(c) || c == b'_' || c == b'-')
}

/// Whether `s` is an app id: `^[a-z0-9][a-z0-9_-]{0,47}$`.
pub fn valid_app(s: &str) -> bool {
    valid_id(s, 48)
}

/// Whether `s` is a process id: `^[a-z0-9][a-z0-9_-]{0,31}$`.
pub fn valid_process(s: &str) -> bool {
    valid_id(s, 32)
}

/// `app` or `app-process`, validated.
pub fn endpoint_id(app: &str, process: Option<&str>) -> Result<String, NameError> {
    if !valid_app(app) {
        return Err(NameError::InvalidApp(app.to_owned()));
    }
    match process {
        None => Ok(app.to_owned()),
        Some(p) if valid_process(p) => Ok(format!("{app}-{p}")),
        Some(p) => Err(NameError::InvalidProcess(p.to_owned())),
    }
}

/// `stintmate-<app>[-<process>]-<user>` for an explicit user (lowercased here): the pipe name
/// without `\\.\pipe\`. This exact string is bound into the handshake MACs.
pub fn short_name_for(app: &str, process: Option<&str>, user: &str) -> Result<String, NameError> {
    Ok(format!(
        "stintmate-{}-{}",
        endpoint_id(app, process)?,
        ascii_lower(user)
    ))
}

/// The short name for the current Windows user.
pub fn short_name(app: &str, process: Option<&str>) -> Result<String, NameError> {
    short_name_for(app, process, &pipe_user()?)
}

/// `\\.\pipe\stintmate-<app>[-<process>]-<user>` for an explicit user.
pub fn pipe_path_for(app: &str, process: Option<&str>, user: &str) -> Result<String, NameError> {
    Ok(format!(
        "{PIPE_PREFIX}{}",
        short_name_for(app, process, user)?
    ))
}

/// The pipe path for the current Windows user.
pub fn pipe_path(app: &str, process: Option<&str>) -> Result<String, NameError> {
    Ok(format!("{PIPE_PREFIX}{}", short_name(app, process)?))
}

/// `(path, short)` for a full `\\.\pipe\...` path or a short name.
pub fn split_pipe(name: &str) -> Result<(String, String), NameError> {
    let short = match name.get(..PIPE_PREFIX.len()) {
        Some(p) if p.eq_ignore_ascii_case(PIPE_PREFIX) => &name[PIPE_PREFIX.len()..],
        _ => name,
    };
    if short.is_empty() || short.contains('\\') {
        return Err(NameError::InvalidPipe(name.to_owned()));
    }
    Ok((format!("{PIPE_PREFIX}{short}"), short.to_owned()))
}

/// A random id per server start (`welcome.run`): 16 lowercase hex digits (64 bits).
pub fn new_run_id() -> String {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b).expect("the system random generator failed");
    b.iter().map(|x| format!("{x:02x}")).collect()
}

// -------------------------------------------------------------------------- workspace

/// The workspace as SPEC 4.3 says, reading the environment through `env`:
/// `STINTMATE_WORKSPACE`, then the pointer file `%APPDATA%\Stintmate\settings.json`
/// (`STINTMATE_SETTINGS_FILE` replaces its path, for tests).
pub fn find_workspace_with(env: &dyn Fn(&str) -> Option<String>) -> Option<PathBuf> {
    let get = |k: &str| {
        env(k)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    };
    if let Some(ws) = get("STINTMATE_WORKSPACE") {
        return Some(PathBuf::from(ws));
    }
    let pointer = match get("STINTMATE_SETTINGS_FILE") {
        Some(p) => PathBuf::from(p),
        None => PathBuf::from(get("APPDATA")?)
            .join("Stintmate")
            .join("settings.json"),
    };
    let bytes = fs::read(pointer).ok()?;
    let text = std::str::from_utf8(bytes.strip_prefix(b"\xef\xbb\xbf").unwrap_or(&bytes)).ok()?;
    let doc: Json = serde_json::from_str(text).ok()?;
    let ws = doc.get("workspace")?.as_str()?;
    (!ws.trim().is_empty()).then(|| PathBuf::from(ws))
}

/// [`find_workspace_with`] on the process environment.
pub fn find_workspace() -> Option<PathBuf> {
    find_workspace_with(&|k| std::env::var(k).ok())
}

/// `<workspace>\ipc`.
pub fn ipc_dir(workspace: &Path) -> PathBuf {
    workspace.join("ipc")
}

// --------------------------------------------------------------------------- registry

/// One `<workspace>\ipc\endpoints\<endpoint id>.json` (SPEC 4.4).
#[derive(Debug, Clone, PartialEq)]
pub struct RegistryEntry {
    /// The endpoint id (`app` or `app-process`).
    pub endpoint: String,
    /// The full pipe path.
    pub pipe: String,
    /// The app id.
    pub app: String,
    /// The process id.
    pub process: Option<String>,
    /// The server's process id.
    pub pid: u32,
    /// The server's `run` id.
    pub run: String,
    /// The wire versions served.
    pub versions: Vec<u32>,
    /// Start time, ISO 8601.
    pub started_at: String,
    /// Display name.
    pub name: String,
    /// Program version.
    pub version: String,
    /// Unknown fields, kept.
    pub extra: Map<String, Json>,
}

impl RegistryEntry {
    /// The JSON document (`schema` 1).
    pub fn to_json(&self) -> Json {
        let mut m = Map::new();
        m.insert("schema".into(), json!(1));
        m.insert("endpoint".into(), json!(self.endpoint));
        m.insert("pipe".into(), json!(self.pipe));
        m.insert("app".into(), json!(self.app));
        m.insert("process".into(), json!(self.process));
        m.insert("pid".into(), json!(self.pid));
        m.insert("run".into(), json!(self.run));
        m.insert("versions".into(), json!(self.versions));
        m.insert("started_at".into(), json!(self.started_at));
        m.insert("name".into(), json!(self.name));
        m.insert("version".into(), json!(self.version));
        for (k, v) in &self.extra {
            m.insert(k.clone(), v.clone());
        }
        Json::Object(m)
    }

    /// Parses an entry; `None` when a required field is missing. Unknown fields are kept.
    pub fn from_json(d: &Json) -> Option<Self> {
        let s = |k: &str| d.get(k).and_then(Json::as_str).map(str::to_owned);
        const KNOWN: [&str; 11] = [
            "schema",
            "endpoint",
            "pipe",
            "app",
            "process",
            "pid",
            "run",
            "versions",
            "started_at",
            "name",
            "version",
        ];
        Some(Self {
            endpoint: s("endpoint")?,
            pipe: s("pipe")?,
            app: s("app")?,
            process: s("process"),
            pid: u32::try_from(d.get("pid")?.as_u64()?).ok()?,
            run: s("run")?,
            versions: d
                .get("versions")
                .and_then(Json::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_u64().and_then(|v| u32::try_from(v).ok()))
                        .collect()
                })
                .unwrap_or_default(),
            started_at: s("started_at").unwrap_or_default(),
            name: s("name").unwrap_or_default(),
            version: s("version").unwrap_or_default(),
            extra: d
                .as_object()?
                .iter()
                .filter(|(k, _)| !KNOWN.contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }

    /// Whether the pipe exists (the only liveness test that counts; the PID may be reused).
    pub fn alive(&self) -> bool {
        security::pipe_exists(&self.pipe)
    }
}

/// `<workspace>\ipc\endpoints`.
pub fn registry_dir(workspace: &Path) -> PathBuf {
    ipc_dir(workspace).join("endpoints")
}

/// Writes the entry atomically (a temporary file in the same folder, then a replace).
pub fn register(directory: &Path, entry: &RegistryEntry) -> io::Result<PathBuf> {
    fs::create_dir_all(directory)?;
    let path = directory.join(format!("{}.json", entry.endpoint));
    let mut r = [0u8; 4];
    getrandom::fill(&mut r).map_err(io::Error::other)?;
    let tmp = directory.join(format!(
        ".{}.{}.{:08x}.tmp",
        entry.endpoint,
        std::process::id(),
        u32::from_le_bytes(r)
    ));
    let text = serde_json::to_string_pretty(&entry.to_json()).map_err(io::Error::other)?;
    fs::write(&tmp, text)?;
    fs::rename(&tmp, &path).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })?;
    Ok(path)
}

/// Deletes the entry if it is still ours (same `run`).
pub fn unregister(directory: &Path, endpoint: &str, run: &str) {
    let path = directory.join(format!("{endpoint}.json"));
    let Ok(text) = fs::read_to_string(&path) else {
        return;
    };
    if let Ok(doc) = serde_json::from_str::<Json>(&text)
        && doc.get("run").and_then(Json::as_str) == Some(run)
    {
        let _ = fs::remove_file(path);
    }
}

/// The entries of the registry; with `alive_only` only those whose pipe exists; `prune`
/// deletes the stale ones (older than 10 s, so a server that is starting is spared).
pub fn list_endpoints(directory: &Path, alive_only: bool, prune: bool) -> Vec<RegistryEntry> {
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(directory) else {
        return out;
    };
    let mut files: Vec<PathBuf> = rd
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    for p in files {
        let Some(entry) = fs::read(&p)
            .ok()
            .and_then(|b| serde_json::from_slice::<Json>(&b).ok())
            .and_then(|d| RegistryEntry::from_json(&d))
        else {
            continue;
        };
        if alive_only || prune {
            let ok = entry.alive();
            if !ok && prune {
                let old = fs::metadata(&p)
                    .and_then(|m| m.modified())
                    .ok()
                    .and_then(|t| SystemTime::now().duration_since(t).ok())
                    .is_some_and(|age| age > Duration::from_secs(10));
                if old {
                    let _ = fs::remove_file(&p);
                }
            }
            if !ok && alive_only {
                continue;
            }
        }
        out.push(entry);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_python_package() {
        assert_eq!(
            pipe_path_for("pitwall", None, "Alex").unwrap(),
            r"\\.\pipe\stintmate-pitwall-alex"
        );
        assert_eq!(
            pipe_path_for("voice", Some("radio"), "alex").unwrap(),
            r"\\.\pipe\stintmate-voice-radio-alex"
        );
        assert_eq!(
            pipe_path_for("connector-ams2", Some("recorder"), "ALEX").unwrap(),
            r"\\.\pipe\stintmate-connector-ams2-recorder-alex"
        );
        assert_eq!(
            short_name_for("pitwall", None, "alex").unwrap(),
            "stintmate-pitwall-alex"
        );
    }

    #[test]
    fn only_ascii_letters_are_lowercased() {
        assert_eq!(ascii_lower("ÁLEX Ñ-Z"), "Álex Ñ-z");
        assert_eq!(ascii_lower("ÀB"), "Àb");
    }

    #[test]
    fn id_rules() {
        assert!(valid_app("a") && valid_app("0x_-y") && valid_app(&"a".repeat(48)));
        assert!(
            !valid_app(&"a".repeat(49)) && !valid_app("") && !valid_app("-a") && !valid_app("A")
        );
        assert!(!valid_app("a.b") && !valid_app("_a"));
        assert!(valid_process(&"p".repeat(32)) && !valid_process(&"p".repeat(33)));
        assert_eq!(
            endpoint_id("a-b", Some("c")).unwrap(),
            endpoint_id("a", Some("b-c")).unwrap()
        );
        assert!(matches!(
            endpoint_id("Bad", None),
            Err(NameError::InvalidApp(_))
        ));
        assert!(matches!(
            endpoint_id("a", Some("X")),
            Err(NameError::InvalidProcess(_))
        ));
    }

    #[test]
    fn split_pipe_accepts_both_forms() {
        let want = (
            r"\\.\pipe\stintmate-x-u".to_owned(),
            "stintmate-x-u".to_owned(),
        );
        assert_eq!(split_pipe(r"\\.\pipe\stintmate-x-u").unwrap(), want);
        assert_eq!(split_pipe(r"\\.\PIPE\stintmate-x-u").unwrap(), want);
        assert_eq!(split_pipe("stintmate-x-u").unwrap(), want);
        assert!(
            split_pipe("").is_err()
                && split_pipe(r"\\.\pipe\").is_err()
                && split_pipe(r"a\b").is_err()
        );
    }

    #[test]
    fn current_user_name_is_used() {
        let user = pipe_user().unwrap();
        assert!(!user.is_empty() && user == user.to_ascii_lowercase());
        assert!(
            pipe_path("pitwall", None)
                .unwrap()
                .ends_with(&format!("-{user}"))
        );
    }

    #[test]
    fn run_ids_are_64_bits_of_hex() {
        let a = new_run_id();
        assert_eq!(a.len(), 16);
        assert!(a.bytes().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, new_run_id());
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "stintmate-ipc-{tag}-{}-{}",
            std::process::id(),
            new_run_id()
        ));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn workspace_resolution_order() {
        let d = temp_dir("ws");
        let settings = d.join("settings.json");
        let mut bytes = b"\xef\xbb\xbf".to_vec();
        bytes.extend_from_slice(br#"{"workspace": "C:\\Work\\Stintmate"}"#);
        fs::write(&settings, bytes).unwrap();
        let sp = settings.to_string_lossy().into_owned();
        let env = |pairs: &'static [(&'static str, &'static str)], sp: String| {
            move |k: &str| {
                if k == "STINTMATE_SETTINGS_FILE" {
                    return Some(sp.clone());
                }
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| (*v).to_owned())
            }
        };
        // the variable wins over the pointer file, blank is ignored
        assert_eq!(
            find_workspace_with(&env(&[("STINTMATE_WORKSPACE", "D:\\Env")], sp.clone())),
            Some(PathBuf::from("D:\\Env"))
        );
        assert_eq!(
            find_workspace_with(&env(&[("STINTMATE_WORKSPACE", "  ")], sp.clone())),
            Some(PathBuf::from("C:\\Work\\Stintmate"))
        );
        // APPDATA is used when no override of the pointer is given
        fs::create_dir_all(d.join("Stintmate")).unwrap();
        fs::write(
            d.join("Stintmate").join("settings.json"),
            r#"{"workspace": "E:\\Ws"}"#,
        )
        .unwrap();
        let appdata = d.to_string_lossy().into_owned();
        let found = find_workspace_with(&move |k| (k == "APPDATA").then(|| appdata.clone()));
        assert_eq!(found, Some(PathBuf::from("E:\\Ws")));
        assert_eq!(find_workspace_with(&|_| None), None);
        fs::write(&settings, "not json").unwrap();
        assert_eq!(find_workspace_with(&env(&[], sp)), None);
        let _ = fs::remove_dir_all(d);
    }

    fn entry(run: &str) -> RegistryEntry {
        let mut extra = Map::new();
        extra.insert("future".into(), json!(true));
        RegistryEntry {
            endpoint: "pitwall".into(),
            pipe: r"\\.\pipe\stintmate-pitwall-alex".into(),
            app: "pitwall".into(),
            process: None,
            pid: 4242,
            run: run.into(),
            versions: vec![2],
            started_at: "2026-10-20T18:00:00+00:00".into(),
            name: "Pitwall".into(),
            version: "0.9.0".into(),
            extra,
        }
    }

    #[test]
    fn registry_round_trip_and_unregister() {
        let d = temp_dir("reg");
        let e = entry("5f1c2a9be03d4471");
        let path = register(&d, &e).unwrap();
        assert_eq!(path, d.join("pitwall.json"));
        let doc: Json = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(doc["schema"], 1);
        assert!(doc["process"].is_null());
        assert_eq!(RegistryEntry::from_json(&doc).unwrap(), e);
        // no temporary file is left behind
        assert_eq!(fs::read_dir(&d).unwrap().count(), 1);
        // a replace works, another run's entry is not removed
        register(&d, &entry("aaaaaaaaaaaaaaaa")).unwrap();
        unregister(&d, "pitwall", "5f1c2a9be03d4471");
        assert!(path.exists());
        unregister(&d, "pitwall", "aaaaaaaaaaaaaaaa");
        assert!(!path.exists());
        let _ = fs::remove_dir_all(d);
    }

    #[test]
    fn listing_skips_dead_pipes_and_prunes_old_stale_entries() {
        let d = temp_dir("list");
        let mut e = entry("aaaaaaaaaaaaaaaa");
        e.pipe = format!(r"\\.\pipe\stintmate-test-{}", new_run_id());
        register(&d, &e).unwrap();
        fs::write(d.join("broken.json"), "{").unwrap();
        assert_eq!(list_endpoints(&d, false, false).len(), 1);
        assert!(list_endpoints(&d, true, false).is_empty());
        // fresh: spared by prune
        assert!(list_endpoints(&d, true, true).is_empty());
        assert!(d.join("pitwall.json").exists());
        assert!(list_endpoints(&d.join("missing"), true, true).is_empty());
        let _ = fs::remove_dir_all(d);
    }
}
