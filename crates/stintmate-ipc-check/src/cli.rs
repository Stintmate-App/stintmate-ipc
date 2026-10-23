//! The command line of `stintmate-ipc-check`.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

use stintmate_ipc::codec::Value;
use stintmate_ipc::keyfile::{self, Key};
use stintmate_ipc::naming;

use crate::client::ClientChecker;
use crate::report::Report;
use crate::server::ServerChecker;

const USAGE: &str = "\
usage: stintmate-ipc-check <command> [options]

commands:
  server <pipe>   check a server endpoint
  client <pipe>   serve a test endpoint and check the client that connects to it
  list            list the endpoints in the workspace's registry
  keygen [path]   create a key file if it does not exist

<pipe>: \\\\.\\pipe\\stintmate-... or the short name (stintmate-pitwall-alex).

common options (server, client):
  --key-file PATH    the key file (default <workspace>\\ipc\\key)
  --key-hex HEX      the key as 64 hex characters (tests)
  --workspace DIR    the Stintmate workspace (for the default key)
  --json             print the report as JSON

server options:
  --listen SECONDS   seconds to listen to each channel (default 2)
  --channel NAME     check only these channels (repeatable)
  --call METHOD[=JSON]  also call this method (may have side effects: your choice)
  --fast             skip the checks that wait for timeouts (handshake deadline, ping)

client options:
  --timeout SECONDS    seconds to wait for the client (default 60)
  --serve SECONDS      seconds to serve the client after the checks (default 5)
  --reconnect SECONDS  seconds to wait for it to reconnect (default 10)

list options:
  --workspace DIR    the workspace
  --all              include stale entries

Exit status 0 when every check passed (warnings allowed), 1 when one failed, 2 on a usage error.";

/// Parsed options: flags, single values and repeated values.
#[derive(Default)]
struct Args {
    positional: Vec<String>,
    values: HashMap<String, Vec<String>>,
    flags: Vec<String>,
}

impl Args {
    fn value(&self, name: &str) -> Option<&str> {
        self.values
            .get(name)
            .and_then(|v| v.last())
            .map(String::as_str)
    }

    fn all(&self, name: &str) -> Vec<String> {
        self.values.get(name).cloned().unwrap_or_default()
    }

    fn flag(&self, name: &str) -> bool {
        self.flags.iter().any(|f| f == name)
    }

    fn seconds(&self, name: &str, default: f64) -> Result<f64, String> {
        match self.value(name) {
            None => Ok(default),
            Some(s) => s
                .parse::<f64>()
                .ok()
                .filter(|v| v.is_finite() && *v >= 0.0)
                .ok_or_else(|| format!("--{name}: {s:?} is not a number of seconds")),
        }
    }
}

fn parse(argv: &[String], flags: &[&str], valued: &[&str]) -> Result<Args, String> {
    let mut out = Args::default();
    let mut i = 0;
    while i < argv.len() {
        let a = &argv[i];
        i += 1;
        let Some(opt) = a.strip_prefix("--") else {
            out.positional.push(a.clone());
            continue;
        };
        let (name, inline) = match opt.split_once('=') {
            Some((n, v)) => (n, Some(v.to_owned())),
            None => (opt, None),
        };
        if flags.contains(&name) && inline.is_none() {
            out.flags.push(name.to_owned());
        } else if valued.contains(&name) {
            let v = match inline {
                Some(v) => v,
                None => {
                    i += 1;
                    argv.get(i - 1)
                        .cloned()
                        .ok_or_else(|| format!("--{name} needs a value"))?
                }
            };
            out.values.entry(name.to_owned()).or_default().push(v);
        } else {
            return Err(format!("unknown option {a}"));
        }
    }
    Ok(out)
}

fn json_value(j: &serde_json::Value) -> Value {
    use serde_json::Value as J;
    match j {
        J::Null => Value::Nil,
        J::Bool(b) => Value::Boolean(*b),
        J::Number(n) => n
            .as_i64()
            .map(Value::from)
            .or_else(|| n.as_u64().map(Value::from))
            .or_else(|| n.as_f64().map(Value::from))
            .unwrap_or(Value::Nil),
        J::String(s) => Value::from(s.as_str()),
        J::Array(a) => Value::Array(a.iter().map(json_value).collect()),
        J::Object(o) => Value::Map(
            o.iter()
                .map(|(k, v)| (Value::from(k.as_str()), json_value(v)))
                .collect(),
        ),
    }
}

fn calls(items: &[String]) -> Result<Vec<(String, Value)>, String> {
    items
        .iter()
        .map(|item| {
            let (name, params) = item.split_once('=').unwrap_or((item, ""));
            let value = if params.is_empty() {
                Value::Map(Vec::new())
            } else {
                let j: serde_json::Value = serde_json::from_str(params)
                    .map_err(|e| format!("--call {name}: invalid JSON: {e}"))?;
                json_value(&j)
            };
            Ok((name.to_owned(), value))
        })
        .collect()
}

fn key(args: &Args) -> Result<Key, String> {
    if let Some(hex) = args.value("key-hex") {
        return keyfile::parse_key(hex.as_bytes()).map_err(|e| e.to_string());
    }
    let path = match args.value("key-file") {
        Some(p) => PathBuf::from(p),
        None => keyfile::default_key_path(args.value("workspace").map(std::path::Path::new))
            .map_err(|e| e.to_string())?,
    };
    keyfile::read_key(&path).map_err(|e| e.to_string())
}

fn emit(report: &Report, json: bool) -> i32 {
    let text = if json {
        report.to_json()
    } else {
        report.to_text()
    };
    let mut out = std::io::stdout().lock();
    let _ = writeln!(out, "{text}");
    let _ = out.flush();
    i32::from(!report.ok())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a Tokio runtime")
}

/// Runs the tool with `argv` (without the program name) and returns the exit status.
pub fn main(argv: &[String]) -> i32 {
    let usage_error = |msg: &str| {
        eprintln!("stintmate-ipc-check: error: {msg}");
        eprintln!("{USAGE}");
        2
    };
    let Some(cmd) = argv.first() else {
        return usage_error("a command is required");
    };
    if matches!(cmd.as_str(), "-h" | "--help" | "help") || argv.iter().any(|a| a == "--help") {
        println!("{USAGE}");
        return 0;
    }
    let rest = &argv[1..];
    let common_flags = ["json"];
    let common_valued = ["key-file", "key-hex", "workspace"];
    match cmd.as_str() {
        "list" => {
            let args = match parse(rest, &["all"], &["workspace"]) {
                Ok(a) => a,
                Err(e) => return usage_error(&e),
            };
            let ws = args
                .value("workspace")
                .map(PathBuf::from)
                .or_else(naming::find_workspace);
            let Some(ws) = ws else {
                eprintln!("no workspace (use --workspace)");
                return 2;
            };
            for e in naming::list_endpoints(&naming::registry_dir(&ws), !args.flag("all"), false) {
                let state = if e.alive() { "alive" } else { "stale" };
                println!(
                    "{:32} {:5} pid {:<6} {}  {} {}",
                    e.endpoint, state, e.pid, e.pipe, e.name, e.version
                );
            }
            0
        }
        "keygen" => {
            let args = match parse(rest, &[], &["workspace"]) {
                Ok(a) => a,
                Err(e) => return usage_error(&e),
            };
            let path = match args.positional.first() {
                Some(p) => PathBuf::from(p),
                None => match keyfile::default_key_path(
                    args.value("workspace").map(std::path::Path::new),
                ) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("error: {e}");
                        return 2;
                    }
                },
            };
            let existed = path.exists();
            match keyfile::ensure_key(&path) {
                Ok(_) => {
                    println!(
                        "{}: {}",
                        path.display(),
                        if existed {
                            "already existed"
                        } else {
                            "created"
                        }
                    );
                    0
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    2
                }
            }
        }
        "server" => {
            let mut flags = vec!["fast"];
            flags.extend(common_flags);
            let mut valued = vec!["listen", "channel", "call"];
            valued.extend(common_valued);
            let args = match parse(rest, &flags, &valued) {
                Ok(a) => a,
                Err(e) => return usage_error(&e),
            };
            let [pipe] = args.positional.as_slice() else {
                return usage_error("server takes exactly one <pipe>");
            };
            let prepared = (|| -> Result<_, String> {
                Ok((
                    key(&args)?,
                    args.seconds("listen", 2.0)?,
                    calls(&args.all("call"))?,
                ))
            })();
            let (key, listen, calls) = match prepared {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 2;
                }
            };
            let channels = args.values.get("channel").cloned();
            let checker =
                ServerChecker::new(pipe, key, listen, !args.flag("fast"), channels, calls);
            let report = on_big_stack(move || {
                let rt = runtime();
                let report = rt.block_on(checker.run());
                rt.shutdown_background();
                report
            });
            emit(&report, args.flag("json"))
        }
        "client" => {
            let mut valued = vec!["timeout", "serve", "reconnect"];
            valued.extend(common_valued);
            let args = match parse(rest, &common_flags, &valued) {
                Ok(a) => a,
                Err(e) => return usage_error(&e),
            };
            let [pipe] = args.positional.as_slice() else {
                return usage_error("client takes exactly one <pipe>");
            };
            let prepared = (|| -> Result<_, String> {
                Ok((
                    key(&args)?,
                    args.seconds("timeout", 60.0)?,
                    args.seconds("serve", 5.0)?,
                    args.seconds("reconnect", 10.0)?,
                ))
            })();
            let (key, timeout, serve, reconnect) = match prepared {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 2;
                }
            };
            let checker = match ClientChecker::new(pipe, key, timeout, serve, reconnect) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("error: {e}");
                    return 2;
                }
            };
            match naming::split_pipe(pipe) {
                Ok((path, _)) => eprintln!(
                    "Serving the test endpoint {path}: start the client now (waiting {timeout} s)."
                ),
                Err(e) => {
                    eprintln!("error: {e}");
                    return 2;
                }
            }
            let report = on_big_stack(move || {
                let rt = runtime();
                let report = rt.block_on(checker.run());
                rt.shutdown_background();
                report
            });
            emit(&report, args.flag("json"))
        }
        other => usage_error(&format!("unknown command {other:?}")),
    }
}

/// Runs `f` on a thread with a large stack: the futures of the checks are big, and the main
/// thread of a Windows process has 1 MiB.
fn on_big_stack(f: impl FnOnce() -> Report + Send + 'static) -> Report {
    std::thread::Builder::new()
        .stack_size(64 * 1024 * 1024)
        .spawn(f)
        .expect("a thread for the checks")
        .join()
        .unwrap_or_else(|_| {
            let mut r = Report::new("check", "");
            r.add(
                "S00",
                "the tool",
                crate::report::Status::Fail,
                "the checks panicked",
                "",
            );
            r
        })
}
