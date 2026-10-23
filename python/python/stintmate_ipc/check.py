"""The conformance tool (``stintmate-ipc-check``).

The checks are written in Rust (``stintmate-ipc-check``, PRY-085 T5); this entry point
arrives when that library does.
"""

from __future__ import annotations

from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Runs the conformance checks and returns the exit code."""
    raise NotImplementedError("stintmate_ipc.check is not available yet (PRY-085 T5)")
