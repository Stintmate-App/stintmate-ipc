"""Stintmate IPC v2 for Python: a binding of the Rust implementation (skeleton, PRY-085 T0)."""

from stintmate_ipc._native import spec_revision, version

__all__ = ["spec_revision", "version"]
__version__ = version()
