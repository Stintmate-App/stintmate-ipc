"""Fixtures: a Rust test endpoint on a random pipe, with a random key."""

from __future__ import annotations

import os
import secrets

import pytest

from stintmate_ipc import Client, PeerInfo, _testing


@pytest.fixture
def key() -> bytes:
    return secrets.token_bytes(32)


@pytest.fixture
def endpoint(key):
    pipe = f"\\\\.\\pipe\\stintmate-t6test-{secrets.token_hex(4)}-{os.getpid()}"
    ep = _testing.start(pipe, key)
    yield ep
    ep.stop()


@pytest.fixture
def client(endpoint, key):
    c = Client(endpoint.path, PeerInfo("t6client", name="T6", version="1"), key)
    c.connect(5.0)
    yield c
    c.close()
