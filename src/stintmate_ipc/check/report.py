"""Check results and their rendering."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

PASS, FAIL, WARN, SKIP, INFO = "PASS", "FAIL", "WARN", "SKIP", "INFO"


@dataclass
class Result:
    id: str
    name: str
    status: str
    detail: str = ""
    spec: str = ""


@dataclass
class Report:
    mode: str
    target: str
    spec_version: str = "2.0"
    tool_version: str = "0.1.0"
    results: list[Result] = field(default_factory=list)
    facts: dict[str, Any] = field(default_factory=dict)

    def add(self, id: str, name: str, status: str, detail: str = "", spec: str = "") -> Result:
        r = Result(id, name, status, detail, spec)
        self.results.append(r)
        return r

    @property
    def ok(self) -> bool:
        return not any(r.status == FAIL for r in self.results)

    def counts(self) -> dict[str, int]:
        out = {s: 0 for s in (PASS, FAIL, WARN, SKIP, INFO)}
        for r in self.results:
            out[r.status] += 1
        return out

    def to_json(self) -> str:
        return json.dumps({"mode": self.mode, "target": self.target,
                           "spec_version": self.spec_version,
                           "tool_version": self.tool_version, "ok": self.ok,
                           "counts": self.counts(), "facts": self.facts,
                           "results": [asdict(r) for r in self.results]}, indent=2,
                          default=repr)

    def to_text(self) -> str:
        lines = [f"stintmate-ipc-check {self.tool_version} - {self.mode} checks against "
                 f"{self.target} (SPEC {self.spec_version})", ""]
        for r in self.results:
            ref = f" [SPEC {r.spec}]" if r.spec else ""
            lines.append(f"{r.status:4}  {r.id}  {r.name}{ref}")
            if r.detail:
                lines.append(f"            {r.detail}")
        c = self.counts()
        lines += ["", f"{c[PASS]} passed, {c[FAIL]} failed, {c[WARN]} warnings, "
                      f"{c[SKIP]} skipped" + ("" if self.ok else "  -> NOT CONFORMANT")]
        if self.facts:
            lines += ["", "Observed:"] + [f"  {k}: {v}" for k, v in self.facts.items()]
        return "\n".join(lines)
