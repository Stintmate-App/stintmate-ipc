//! Check results and their rendering (text and JSON).

use serde_json::{Map, Value as Json, json};

/// The status of one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// The check passed.
    Pass,
    /// The implementation broke the protocol.
    Fail,
    /// Suspicious, not a violation.
    Warn,
    /// Not run (nothing to check, or a precondition missing).
    Skip,
    /// Information only.
    Info,
}

impl Status {
    /// The name in reports.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Fail => "FAIL",
            Self::Warn => "WARN",
            Self::Skip => "SKIP",
            Self::Info => "INFO",
        }
    }
}

/// One line of the report.
#[derive(Debug, Clone)]
pub struct CheckResult {
    /// The check id (`S07`, `C03`, `S19.<channel>`).
    pub id: String,
    /// What was checked.
    pub name: String,
    /// The outcome.
    pub status: Status,
    /// Details for people.
    pub detail: String,
    /// The SPEC sections.
    pub spec: String,
}

/// The results of one run and the facts observed.
#[derive(Debug, Clone)]
pub struct Report {
    /// `server` or `client`.
    pub mode: String,
    /// What was checked (the pipe path).
    pub target: String,
    /// Results in order.
    pub results: Vec<CheckResult>,
    /// Observed facts, in the order they were found.
    pub facts: Map<String, Json>,
}

impl Report {
    /// An empty report.
    pub fn new(mode: &str, target: &str) -> Self {
        Self {
            mode: mode.to_owned(),
            target: target.to_owned(),
            results: Vec::new(),
            facts: Map::new(),
        }
    }

    /// Appends a result.
    pub fn add(&mut self, id: &str, name: &str, status: Status, detail: &str, spec: &str) {
        self.results.push(CheckResult {
            id: id.to_owned(),
            name: name.to_owned(),
            status,
            detail: detail.to_owned(),
            spec: spec.to_owned(),
        });
    }

    /// Records a fact.
    pub fn fact(&mut self, key: &str, value: Json) {
        self.facts.insert(key.to_owned(), value);
    }

    /// Whether no check failed (warnings are allowed).
    pub fn ok(&self) -> bool {
        !self.results.iter().any(|r| r.status == Status::Fail)
    }

    /// How many results have each status: pass, fail, warn, skip, info.
    pub fn counts(&self) -> [usize; 5] {
        let mut c = [0; 5];
        for r in &self.results {
            c[r.status as usize] += 1;
        }
        c
    }

    /// The report as JSON (indented by two).
    pub fn to_json(&self) -> String {
        let c = self.counts();
        let results: Vec<Json> = self
            .results
            .iter()
            .map(|r| {
                json!({"id": r.id, "name": r.name, "status": r.status.name(),
                       "detail": r.detail, "spec": r.spec})
            })
            .collect();
        let v = json!({
            "mode": self.mode, "target": self.target,
            "spec_version": stintmate_ipc::SPEC_REVISION,
            "tool_version": stintmate_ipc::VERSION, "ok": self.ok(),
            "counts": {"PASS": c[0], "FAIL": c[1], "WARN": c[2], "SKIP": c[3], "INFO": c[4]},
            "facts": self.facts, "results": results,
        });
        serde_json::to_string_pretty(&v).expect("a report is JSON")
    }

    /// The report for people.
    pub fn to_text(&self) -> String {
        let mut lines = vec![
            format!(
                "stintmate-ipc-check {} - {} checks against {} (SPEC {})",
                stintmate_ipc::VERSION,
                self.mode,
                self.target,
                stintmate_ipc::SPEC_REVISION
            ),
            String::new(),
        ];
        for r in &self.results {
            let spec = if r.spec.is_empty() {
                String::new()
            } else {
                format!(" [SPEC {}]", r.spec)
            };
            lines.push(format!(
                "{:4}  {}  {}{}",
                r.status.name(),
                r.id,
                r.name,
                spec
            ));
            if !r.detail.is_empty() {
                lines.push(format!("            {}", r.detail));
            }
        }
        let c = self.counts();
        lines.push(String::new());
        lines.push(format!(
            "{} passed, {} failed, {} warnings, {} skipped{}",
            c[0],
            c[1],
            c[2],
            c[3],
            if self.ok() { "" } else { "  -> NOT CONFORMANT" }
        ));
        if !self.facts.is_empty() {
            lines.push(String::new());
            lines.push("Observed:".to_owned());
            for (k, v) in &self.facts {
                lines.push(format!("  {k}: {v}"));
            }
        }
        lines.join("\n")
    }
}
