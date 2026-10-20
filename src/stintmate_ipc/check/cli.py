"""``stintmate-ipc-check``: conformance checks for Stintmate IPC v2 implementations.

    stintmate-ipc-check server <pipe> --key-file <key>   # check a server (an endpoint)
    stintmate-ipc-check client <pipe> --key-file <key>   # serve a test endpoint, check a client
    stintmate-ipc-check list [--workspace <dir>]         # endpoints in the registry
    stintmate-ipc-check keygen <path>                    # create a key file (if missing)

``<pipe>``: ``\\\\.\\pipe\\stintmate-...`` or the short name (``stintmate-pitwall-leona``).
The key defaults to ``<workspace>\\ipc\\key``. Exit status 0 when every check passed (warnings
allowed), 1 when one failed, 2 on a usage error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from .. import keyfile, naming
from ..errors import KeyFileError
from .client import ClientChecker
from .server import ServerChecker


def _key(args: argparse.Namespace) -> bytes:
    if args.key_hex:
        return bytes.fromhex(args.key_hex)
    path = Path(args.key_file) if args.key_file else keyfile.default_key_path(args.workspace)
    return keyfile.read_key(path)


def _calls(items: list[str]) -> list[tuple[str, Any]]:
    out = []
    for item in items:
        name, _, params = item.partition("=")
        out.append((name, json.loads(params) if params else {}))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="stintmate-ipc-check",
                                 description="Conformance checks for Stintmate IPC v2.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p: argparse.ArgumentParser) -> None:
        p.add_argument("--key-file", help="the key file (default <workspace>\\ipc\\key)")
        p.add_argument("--key-hex", help="the key as 64 hex characters (tests)")
        p.add_argument("--workspace", help="the Stintmate workspace (for the default key)")
        p.add_argument("--json", action="store_true", help="print the report as JSON")

    s = sub.add_parser("server", help="check a server endpoint")
    s.add_argument("pipe")
    common(s)
    s.add_argument("--listen", type=float, default=2.0,
                   help="seconds to listen to each channel (default 2)")
    s.add_argument("--channel", action="append", help="check only these channels")
    s.add_argument("--call", action="append", default=[], metavar="METHOD[=JSON]",
                   help="also call this method (may have side effects: your choice)")
    s.add_argument("--fast", action="store_true",
                   help="skip the checks that wait for timeouts (handshake deadline, ping)")

    c = sub.add_parser("client", help="serve a test endpoint and check the client that "
                                      "connects to it")
    c.add_argument("pipe")
    common(c)
    c.add_argument("--timeout", type=float, default=60.0,
                   help="seconds to wait for the client (default 60)")
    c.add_argument("--serve", type=float, default=5.0,
                   help="seconds to serve the client after the checks (default 5)")
    c.add_argument("--reconnect", type=float, default=10.0,
                   help="seconds to wait for it to reconnect (default 10)")

    ls = sub.add_parser("list", help="list the endpoints in the workspace's registry")
    ls.add_argument("--workspace")
    ls.add_argument("--all", action="store_true", help="include stale entries")

    kg = sub.add_parser("keygen", help="create a key file if it does not exist")
    kg.add_argument("path", nargs="?")
    kg.add_argument("--workspace")

    args = ap.parse_args(argv)
    try:
        if args.cmd == "list":
            ws = Path(args.workspace) if args.workspace else naming.find_workspace()
            if ws is None:
                print("no workspace (use --workspace)", file=sys.stderr)
                return 2
            for e in naming.list_endpoints(naming.registry_dir(ws), alive_only=not args.all):
                state = "alive" if e.alive() else "stale"
                print(f"{e.endpoint:32} {state:5} pid {e.pid:<6} {e.pipe}  {e.name} "
                      f"{e.version}")
            return 0
        if args.cmd == "keygen":
            path = Path(args.path) if args.path else keyfile.default_key_path(args.workspace)
            existed = path.exists()
            keyfile.ensure_key(path)
            print(f"{path}: {'already existed' if existed else 'created'}")
            return 0
        key = _key(args)
    except (KeyFileError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if args.cmd == "server":
        report = ServerChecker(args.pipe, key, listen_s=args.listen, slow=not args.fast,
                               channels=args.channel, calls=_calls(args.call)).run()
    else:
        print(f"Serving the test endpoint {naming.split_pipe(args.pipe)[0]}: start the client "
              f"now (waiting {args.timeout:g} s).", file=sys.stderr)
        report = ClientChecker(args.pipe, key, timeout=args.timeout, serve_s=args.serve,
                               reconnect_s=args.reconnect).run()
    print(report.to_json() if args.json else report.to_text())
    return 0 if report.ok else 1


def run() -> None:
    """The console entry point. The checks leave pipe and worker threads behind when the
    endpoint (or the client) goes away first; the report is printed by now, so flush it
    and leave at once instead of waiting for them (the tool never hangs at exit)."""
    try:
        code = main()
    except KeyboardInterrupt:
        code = 130
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except (OSError, ValueError):
            pass
    os._exit(code)


if __name__ == "__main__":
    run()
