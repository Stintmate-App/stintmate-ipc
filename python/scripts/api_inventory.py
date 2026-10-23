r"""List every `stintmate_ipc` name and attribute the monorepo's Python sources use.

Read-only over the monorepo. Usage: python api_inventory.py <monorepo-root> > API_INVENTORY.md

It parses each file with `ast`: imports (`from stintmate_ipc[.sub] import X`, `import stintmate_ipc
[as m]`) give the names; then the attribute accesses on objects built from them are reported by
attribute name (`.call`, `.subscribe`, ...), grouped by the imported class they are most
likely used on (a plain attribute-name census: a name used on any object counts).
"""

from __future__ import annotations

import ast
import sys
from collections import defaultdict
from pathlib import Path

SRC_ROOTS = ["apps/*/src", "apps/connectors/ams2/src", "examples/connector-skeleton", "tools/integration"]
TEST_ROOTS = ["apps/*/tests", "apps/connectors/ams2/tests", "examples/connector-skeleton/tests", "tools/integration"]

# attributes to watch for (names that only make sense on library objects); everything that
# follows `.` on a variable is too noisy, so we report names that appear in the known surface
# plus any attribute on a direct `stintmate_ipc.<x>` / `<alias>.<x>` access.
KNOWN_METHODS = {
    "call", "call_async", "subscribe", "close", "stop", "start", "connect", "method", "event_channel",
    "state_channel", "stream_channel", "publish", "send", "set", "value", "on_connect", "on_disconnect",
    "on_redeclare", "on_handshake_failed", "on_subscribe", "on_active", "wait_connected", "connected",
    "peer", "peers", "reconnect_delay", "ping", "send_message", "send_to", "send_values", "remove_method",
    "result", "exception", "cancel", "done", "add_done_callback", "on_progress", "is_open", "pid", "info",
    "decl", "subscriber_count", "active", "wait_cancelled", "remaining", "expired", "progress", "cancelled",
    "idempotency_key", "deadline", "params", "version", "id", "error", "declarations", "path", "pack",
    "unpack", "size", "fields", "code", "message", "data", "retry_ms", "app", "process", "name", "meta",
    "layout", "options", "accept", "channel", "seq", "ts", "t_ns", "body", "dropped", "snapshot",
    "values", "reject", "last_error", "key", "registry", "heartbeat", "handler_threads", "deliver",
}


def files(root: Path, patterns: list[str]) -> list[Path]:
    seen: dict[Path, None] = {}
    for pat in patterns:
        for base in root.glob(pat):
            if base.is_file() and base.suffix == ".py":
                seen[base] = None
            else:
                for p in base.rglob("*.py"):
                    if ".venv" in p.parts or "__pycache__" in p.parts or "node_modules" in p.parts:
                        continue
                    seen[p] = None
    return sorted(seen)


class Visitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.imports: set[str] = set()  # "stintmate_ipc.X" / "stintmate_ipc.sub.X"
        self.modules: set[str] = set()  # aliases of the module
        self.attrs: dict[str, int] = defaultdict(int)  # attribute names used on anything
        self.kwargs: dict[str, set[str]] = defaultdict(set)  # callee name -> keywords
        self.imported: dict[str, str] = {}  # local name -> qualified

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        mod = node.module or ""
        if node.level == 0 and (mod == "stintmate_ipc" or mod.startswith("stintmate_ipc.")):
            for a in node.names:
                self.imports.add(f"{mod}.{a.name}")
                self.imported[a.asname or a.name] = f"{mod}.{a.name}"

    def visit_Import(self, node: ast.Import) -> None:
        for a in node.names:
            if a.name == "stintmate_ipc" or a.name.startswith("stintmate_ipc."):
                self.imports.add(a.name)
                self.modules.add(a.asname or a.name)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        # direct module access: stintmate_ipc.X
        if isinstance(node.value, ast.Name) and node.value.id in self.modules:
            self.imports.add(f"stintmate_ipc.{node.attr}")
        elif isinstance(node.value, ast.Attribute):
            chain = dotted(node.value)
            if chain and chain.split(".")[0] in self.modules:
                self.imports.add(f"stintmate_ipc.{chain.split('.', 1)[1]}.{node.attr}")
        if node.attr in KNOWN_METHODS:
            self.attrs[node.attr] += 1
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        name = dotted(node.func)
        if name:
            last = name.split(".")[-1]
            for kw in node.keywords:
                if kw.arg and (last in KNOWN_METHODS or name in self.imported or last in {q.rsplit(".", 1)[-1] for q in self.imported.values()}):
                    self.kwargs[last].add(kw.arg)
        self.generic_visit(node)


def dotted(node: ast.AST) -> str | None:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
        return ".".join(reversed(parts))
    return None


def scan(paths: list[Path], root: Path) -> tuple[dict[str, set[str]], dict[str, int], dict[str, set[str]]]:
    names: dict[str, set[str]] = defaultdict(set)
    attrs: dict[str, int] = defaultdict(int)
    kwargs: dict[str, set[str]] = defaultdict(set)
    for p in paths:
        try:
            tree = ast.parse(p.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):
            continue
        v = Visitor()
        v.visit(tree)
        if not v.imports:
            continue
        rel = p.relative_to(root).as_posix()
        for n in v.imports:
            names[n].add(rel)
        for a, c in v.attrs.items():
            attrs[a] += c
        for k, s in v.kwargs.items():
            kwargs[k] |= s
    return names, attrs, kwargs


def report(title: str, names, attrs, kwargs) -> list[str]:
    out = [f"## {title}", ""]
    out.append("### Imported names")
    out.append("")
    out.append("| Name | Files | Example |")
    out.append("|---|---|---|")
    for n in sorted(names):
        fs = sorted(names[n])
        out.append(f"| `{n}` | {len(fs)} | `{fs[0]}` |")
    out.append("")
    out.append("### Attribute names used (in files that import the library)")
    out.append("")
    out.append(", ".join(f"`{a}` ({c})" for a, c in sorted(attrs.items())))
    out.append("")
    out.append("### Keyword arguments seen on library-ish calls")
    out.append("")
    for k in sorted(kwargs):
        out.append(f"- `{k}(...)`: " + ", ".join(f"`{x}`" for x in sorted(kwargs[k])))
    out.append("")
    return out


def main(argv: list[str]) -> int:
    root = Path(argv[1] if len(argv) > 1 else ".").resolve()
    tests = set(files(root, TEST_ROOTS)) - set()
    all_src = files(root, SRC_ROOTS)
    test_files = sorted(p for p in all_src + sorted(tests) if "tests" in p.parts or p.name.startswith("test_") or p.name in {"conftest.py", "fakes.py", "fake_app.py"})
    src_files = sorted(p for p in set(all_src) if p not in set(test_files))
    lines = ["# API inventory", "", "Generated by `python/scripts/api_inventory.py` over the monorepo (read-only).", ""]
    lines += report("Sources", *scan(src_files, root))
    lines += report("Tests (separately)", *scan(sorted(set(test_files)), root))
    notes = Path(__file__).with_name("_notes.txt")
    if notes.exists():
        lines += ["", notes.read_text(encoding="utf-8")]
    sys.stdout.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
