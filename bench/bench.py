"""Benchmarks of the reference implementation (BENCHMARKS.md).

    uv run python bench/bench.py            # every scenario, prints a Markdown table
    uv run python bench/bench.py --quick    # shorter runs
    uv run python bench/bench.py --json out.json

Every role is a separate process (the same interpreter). Latency is the receiver's
``perf_counter_ns()`` minus the sender's stamp (QueryPerformanceCounter: one clock for every
process on the PC). Scenarios, as in the PRY-056 research (2026-10-19):

- ``rr``: 5000 request/response round trips (``bench.echo``, ~45-byte params), after 200
  warm-up calls, measured by the caller.
- ``event``: 1000 events (~100 bytes) at 200 Hz to one subscriber.
- ``stream``: 200 Hz samples of a 48-byte layout (12 x f32) for 30 s to one subscriber,
  plus the CPU of both processes over the run.
- ``stream3``: the same for 15 s to three subscribers.

CPU % = process time (user + system) / wall time, 100 % = one core. Windows accounts CPU in
15.6 ms ticks: values under ~0.3 % are noise.
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

from stintmate_ipc import Client, Endpoint, Layout, PeerInfo

now = time.perf_counter_ns
LAYOUT = Layout([("v", "f32", 12)], name="bench48")
WARMUP = 200


def sleep_until(t: float) -> None:
    d = t - time.perf_counter()
    if d > 0:
        time.sleep(d)  # 3.11+: high-resolution waitable timer on Windows


def pct(values: list[float], q: float) -> float:
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * q))]


def summary(lat_ns: list[int]) -> dict[str, float]:
    us = [x / 1000 for x in lat_ns]
    return {"n": len(us), "p50": pct(us, 0.50), "p95": pct(us, 0.95), "p99": pct(us, 0.99),
            "max": max(us), "mean": statistics.fmean(us)}


# ---------------------------------------------------------------------------- roles


def role_server(pipe: str, key: bytes, mode: str, n: int, rate: float, subs: int) -> None:
    ep = Endpoint(PeerInfo("bench", name="bench server"), key, pipe=pipe)

    @ep.method("bench.echo", inline=os.environ.get("BENCH_INLINE") == "1")
    def echo(ctx, params):
        return {"echo": params}

    events = ep.event_channel("bench.events")
    stream = ep.stream_channel("bench.stream", LAYOUT, rate_hz=rate)
    ep.start()
    print("ready", flush=True)
    if mode == "rr":
        sys.stdin.readline()  # the runner closes stdin when done
        ep.stop()
        return
    ch = events if mode == "event" else stream
    deadline = time.monotonic() + 30
    while ch.subscriber_count < subs and time.monotonic() < deadline:
        time.sleep(0.01)
    time.sleep(0.5)
    period = 1.0 / rate
    values = [1.5] * 12
    late: list[float] = []
    c0, w0 = time.process_time(), time.perf_counter()
    t = time.perf_counter()
    for i in range(n):
        t += period
        sleep_until(t)
        late.append(time.perf_counter() - t)
        if mode == "event":
            events.publish({"lap": i, "time_s": 91.234, "position": 3, "valid": True,
                            "sent_ns": now(), "car": "examplesim-gt3", "track": "lakeside"})
        else:
            stream.send(LAYOUT.pack(*values))
    wall = time.perf_counter() - w0
    cpu = 100 * (time.process_time() - c0) / wall
    time.sleep(1.0)
    print(json.dumps({"cpu_pct": cpu, "pace_late_us_p50": pct(late, 0.5) * 1e6,
                      "pace_late_us_p99": pct(late, 0.99) * 1e6}), flush=True)
    sys.stdin.readline()
    ep.stop()


def role_rr(pipe: str, key: bytes, n: int) -> None:
    c = Client(pipe, PeerInfo("bench-client"), key)
    c.connect(10)
    params = {"x": 1, "car": "examplesim-gt3", "lap": 12, "ok": True}
    lat: list[int] = []
    for i in range(n + WARMUP):
        t = now()
        c.call("bench.echo", params)
        lat.append(now() - t)
    c.close()
    print(json.dumps({"lat_ns": lat[WARMUP:]}), flush=True)


def role_sub(pipe: str, key: bytes, mode: str, n: int, deliver: str) -> None:
    c = Client(pipe, PeerInfo("bench-sub"), key)
    c.connect(10)
    lat: list[int] = []
    done = threading.Event()
    first: list[float] = []

    if mode == "event":
        def on(ev):
            lat.append(now() - ev.data["sent_ns"])
            if len(lat) == 1:
                first.append(time.process_time())
                first.append(time.perf_counter())
            if len(lat) >= n:
                done.set()
        c.subscribe("bench.events", on, deliver=deliver)
    else:
        def on(s):
            lat.append(now() - s.t_ns)
            if len(lat) == 1:
                first.append(time.process_time())
                first.append(time.perf_counter())
            if len(lat) >= n:
                done.set()
        c.subscribe("bench.stream", on, deliver=deliver)
    done.wait(n / 100 + 60)
    cpu = 100 * (time.process_time() - first[0]) / (time.perf_counter() - first[1]) \
        if first else 0.0
    c.close()
    print(json.dumps({"lat_ns": lat, "cpu_pct": cpu}), flush=True)


# --------------------------------------------------------------------------- runner


def spawn(*args: str) -> subprocess.Popen[str]:
    return subprocess.Popen([sys.executable, __file__, *args], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, text=True)


def run_scenario(mode: str, n: int, rate: float, subs: int, deliver: str) -> dict:
    pipe = "stintmate-bench-" + secrets.token_hex(4)
    key = secrets.token_hex(32)
    server = spawn("server", pipe, key, mode, str(n), str(rate), str(subs))
    assert server.stdout is not None and server.stdin is not None
    assert server.stdout.readline().strip() == "ready"
    if mode == "rr":
        c = spawn("rr", pipe, key, str(n))
        out, _ = c.communicate(timeout=300)
        server.stdin.close()
        server.wait(30)
        return {"mode": mode, "lat": summary(json.loads(out)["lat_ns"])}
    clients = [spawn("sub", pipe, key, mode, str(n), deliver) for _ in range(subs)]
    pub = json.loads(server.stdout.readline())
    results = [json.loads(c.communicate(timeout=n / rate + 120)[0]) for c in clients]
    server.stdin.close()
    server.wait(30)
    lat = [x for r in results for x in r["lat_ns"]]
    return {"mode": mode, "subs": subs, "deliver": deliver, "lat": summary(lat),
            "received": [len(r["lat_ns"]) for r in results], "sent": n,
            "cpu_pub_pct": pub["cpu_pct"], "cpu_sub_pct": [r["cpu_pct"] for r in results],
            "pace_late_us_p50": pub["pace_late_us_p50"],
            "pace_late_us_p99": pub["pace_late_us_p99"]}


def merge(reps: list[dict]) -> dict:
    """Medians over repeated runs (latency percentiles and CPU), the max of the maxima."""
    if len(reps) == 1:
        return reps[0]
    out = dict(reps[0])
    out["lat"] = {k: (max if k == "max" else statistics.median)(r["lat"][k] for r in reps)
                  for k in reps[0]["lat"]}
    out["lat"]["n"] = sum(r["lat"]["n"] for r in reps)
    if "cpu_pub_pct" in out:
        out["cpu_pub_pct"] = statistics.median(r["cpu_pub_pct"] for r in reps)
        out["cpu_sub_pct"] = [statistics.median(r["cpu_sub_pct"][i] for r in reps)
                              for i in range(len(reps[0]["cpu_sub_pct"]))]
        out["received"] = [sum(r["received"][i] for r in reps)
                           for i in range(len(reps[0]["received"]))]
        out["sent"] = sum(r["sent"] for r in reps)
    out["repeats"] = len(reps)
    out["each"] = reps
    return out


def bench_sweep(n: int = 1000, repeat: int = 200) -> dict:
    """One deadline sweep (``Peer._expire_calls``, run every 0.25 s by the heartbeat thread)
    over ``n`` in-flight calls, none expired: the cost of the worst case, in microseconds."""
    from stintmate_ipc.peer import PendingCall
    from stintmate_ipc import Client, Endpoint, PeerInfo
    key = os.urandom(32)
    pipe = f"stintmate-bench-{os.getpid()}"
    ep = Endpoint(PeerInfo("benchsrv"), key, pipe=pipe)
    ep.start()
    c = Client(pipe, PeerInfo("benchcli"), key)
    peer = c.connect(5)
    with peer._lock:
        for i in range(n):
            peer._calls[10_000 + i] = PendingCall(peer, 10_000 + i, "b.x", 600.0, None)
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter_ns()
        peer._expire_calls(time.perf_counter())
        times.append(time.perf_counter_ns() - t0)
    with peer._lock:
        peer._calls.clear()
    c.close()
    ep.stop()
    times.sort()
    return {"n": n, "p50_us": times[len(times) // 2] / 1000,
            "p99_us": times[int(len(times) * 0.99)] / 1000, "max_us": times[-1] / 1000}


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] in ("server", "rr", "sub"):
        role, args = sys.argv[1], sys.argv[2:]
        if role == "server":
            role_server(args[0], bytes.fromhex(args[1]), args[2], int(args[3]),
                        float(args[4]), int(args[5]))
        elif role == "rr":
            role_rr(args[0], bytes.fromhex(args[1]), int(args[2]))
        else:
            role_sub(args[0], bytes.fromhex(args[1]), args[2], int(args[3]), args[4])
        return 0
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--quick", action="store_true", help="shorter runs")
    ap.add_argument("--json", type=Path, help="write the results here")
    ap.add_argument("--deliver", default="auto", choices=("auto", "thread", "inline"))
    ap.add_argument("--only", default="rr,event,stream,stream3",
                    help="comma-separated scenarios (rr, event, stream, stream3; sweep: the deadline sweep, not run by default)")
    ap.add_argument("--scale", type=float, default=1.0, help="multiply every n")
    ap.add_argument("--repeat", type=int, default=1,
                    help="run each scenario N times and report the medians")
    a = ap.parse_args()
    q = (0.2 if a.quick else 1.0) * a.scale
    plan = {"rr": ("rr", 5000, 0, 0), "event": ("event", 1000, 200, 1),
            "stream": ("stream", 6000, 200, 1), "stream3": ("stream", 3000, 200, 3)}
    runs = []
    for k in a.only.split(","):
        if k == "sweep":
            r = bench_sweep()
            print(f"sweep over {r['n']} in-flight calls: p50 {r['p50_us']:.0f} us, "
                  f"p99 {r['p99_us']:.0f} us, max {r['max_us']:.0f} us")
            continue
        m, n, rate, subs = plan[k]
        reps = [run_scenario(m, int(n * q), rate, subs, a.deliver) for _ in range(a.repeat)]
        runs.append(merge(reps))
    meta = {"python": sys.version.split()[0], "pid": os.getpid(),
            "date": time.strftime("%Y-%m-%d %H:%M"), "deliver": a.deliver, "quick": a.quick}
    print("| Scenario | n | p50 us | p95 us | p99 us | max us | CPU pub % | CPU sub % |")
    print("|---|---|---|---|---|---|---|---|")
    for r in runs:
        lat = r["lat"]
        name = r["mode"] + (f" x{r['subs']}" if r.get("subs", 1) > 1 else "")
        cpub = f"{r['cpu_pub_pct']:.2f}" if "cpu_pub_pct" in r else "-"
        csub = " / ".join(f"{x:.2f}" for x in r["cpu_sub_pct"]) if "cpu_sub_pct" in r else "-"
        print(f"| {name} | {lat['n']} | {lat['p50']:.0f} | {lat['p95']:.0f} | "
              f"{lat['p99']:.0f} | {lat['max']:.0f} | {cpub} | {csub} |")
        if "received" in r and any(x != r["sent"] for x in r["received"]):
            print(f"  (!) {name}: received {r['received']} of {r['sent']}")
    if a.json:
        a.json.parent.mkdir(parents=True, exist_ok=True)
        a.json.write_text(json.dumps({"meta": meta, "runs": runs}, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
