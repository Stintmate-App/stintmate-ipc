"""A tiny Rust endpoint for the tests (not API)."""

from stintmate_ipc._native import TestEndpoint, start

__all__ = ["TestEndpoint", "start"]
