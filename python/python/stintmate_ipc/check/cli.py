"""``stintmate-ipc-check``: the conformance checks of the Rust crate ``stintmate-ipc-check``.

    stintmate-ipc-check server <pipe> --key-file <key>   # check a server (an endpoint)
    stintmate-ipc-check client <pipe> --key-file <key>   # serve a test endpoint, check a client
    stintmate-ipc-check list [--workspace <dir>]         # endpoints in the registry
    stintmate-ipc-check keygen <path>                    # create a key file (if missing)

Exit status 0 when every check passed (warnings allowed), 1 when one failed, 2 on a usage
error. ``stintmate-ipc-check --help`` lists the options.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Sequence

from stintmate_ipc import _native


def main(argv: Sequence[str] | None = None) -> int:
    """Runs the conformance tool with ``argv`` (default ``sys.argv[1:]``); returns its exit
    status. The GIL is released while the checks run."""
    args = list(sys.argv[1:] if argv is None else argv)
    status = _native.check_main(args)
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except (OSError, ValueError):
            pass
    return status


def run() -> None:
    """The console entry point: leaves at once, as the checks may leave pipe tasks behind."""
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
