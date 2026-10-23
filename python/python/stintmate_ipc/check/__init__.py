"""The conformance tool (``stintmate-ipc-check``); the checks are written in Rust."""

from stintmate_ipc.check import cli
from stintmate_ipc.check.cli import main, run

__all__ = ["cli", "main", "run"]
