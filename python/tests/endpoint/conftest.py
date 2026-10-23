"""Shared fixtures: a random key and unique pipe names per test, so tests never meet the
driver's running apps (their pipes are ``stintmate-<app>-<user>``)."""

from __future__ import annotations

import secrets
import sys
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from stintmate_ipc import Client, Endpoint, PeerInfo

if sys.platform != "win32":  # pragma: no cover
    pytest.skip("Windows named pipes only", allow_module_level=True)


@pytest.fixture
def key() -> bytes:
    return secrets.token_bytes(32)


@pytest.fixture
def pipe_name() -> str:
    return f"stintmate-test-{secrets.token_hex(6)}"


@pytest.fixture
def endpoint(key: bytes, pipe_name: str) -> Iterator[Callable[..., Endpoint]]:
    """``endpoint(**kw)``: an Endpoint on the test pipe (not started), stopped at the end."""
    made: list[Endpoint] = []

    def make(**kw: Any) -> Endpoint:
        kw.setdefault("pipe", pipe_name)
        info = kw.pop("info", PeerInfo("testsrv", name="test server", version="1.0"))
        ep = Endpoint(info, kw.pop("key", key), **kw)
        made.append(ep)
        return ep

    yield make
    for ep in made:
        ep.stop()


@pytest.fixture
def client(key: bytes, pipe_name: str) -> Iterator[Callable[..., Client]]:
    """``client(**kw)``: a Client of the test pipe (not started), closed at the end."""
    made: list[Client] = []

    def make(**kw: Any) -> Client:
        pipe = kw.pop("pipe", pipe_name)
        info = kw.pop("info", PeerInfo("testcli", name="test client", version="1.0"))
        c = Client(pipe, info, kw.pop("key", key), **kw)
        made.append(c)
        return c

    yield make
    for c in made:
        c.close()


def wait_for(cond: Callable[[], Any], timeout: float = 5.0, interval: float = 0.005) -> Any:
    end = time.monotonic() + timeout
    while True:
        v = cond()
        if v:
            return v
        if time.monotonic() > end:
            raise AssertionError("condition not met in time")
        time.sleep(interval)
