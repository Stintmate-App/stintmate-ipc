"""Measures the call round trip (Python client, Rust endpoint) and the codec from/to Python objects."""

import os
import secrets
import statistics
import time

from stintmate_ipc import Client, PeerInfo, _native, _testing

REQ = {"t": "req", "id": 12345, "m": "pitwall.write_lap", "v": 1, "dl": 5000,
       "p": {"lap": 7, "time_ms": 91234, "sectors": [30123, 30456, 30655], "car": "formula_ultimate", "valid": True}}


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * q))]


def main() -> None:
    key = secrets.token_bytes(32)
    pipe = "\\\\.\\pipe\\stintmate-bench-" + f"{secrets.token_hex(4)}-{os.getpid()}"
    ep = _testing.start(pipe, key)
    c = Client(pipe, PeerInfo("bench"), key)
    c.connect(5)
    for _ in range(500):
        c.call("t.echo", {"x": 1})
    runs = []
    for _ in range(3):
        lat = []
        for _ in range(5000):
            t0 = time.perf_counter_ns()
            c.call("t.echo", {"x": 1})
            lat.append((time.perf_counter_ns() - t0) / 1000)
        runs.append((statistics.median(lat), pct(lat, 0.99)))
    print("call rr p50/p99 us per run:", [(round(a, 1), round(b, 1)) for a, b in runs])
    c.close()
    ep.stop()
    data = _native._encode(REQ)
    print("req bytes:", len(data))
    for name, fn, arg in (("encode", _native._encode, REQ), ("decode", _native._decode, data)):
        res = []
        for _ in range(3):
            n = 200000
            t0 = time.perf_counter()
            for _ in range(n):
                fn(arg)
            res.append((time.perf_counter() - t0) / n * 1e6)
        print(f"codec {name} us/op per run:", [round(x, 2) for x in res])


if __name__ == "__main__":
    main()
