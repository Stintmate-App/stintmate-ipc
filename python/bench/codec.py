"""Codec micro-benchmark: Python objects <-> MessagePack, `req` message of ~109 bytes.

    python bench/codec.py            (compares with the `msgpack` package when installed)
"""

import timeit

from stintmate_ipc import _native

REQ = {"t": "req", "id": 12345, "m": "pitwall.write_lap", "v": 1, "dl": 5000,
       "p": {"car": "examplesim-gt3", "lap": 12, "time_s": 91.234, "ok": True}}


def best(fn, n=200_000, rep=5):
    return min(timeit.repeat(fn, number=n, repeat=rep)) / n * 1e6


def main() -> None:
    data = _native._encode(REQ)
    assert _native._decode(data) == REQ
    print(f"req = {len(data)} bytes")
    print(f"stintmate_ipc encode {best(lambda: _native._encode(REQ)):.2f} us")
    print(f"stintmate_ipc decode {best(lambda: _native._decode(data)):.2f} us")
    try:
        import msgpack
    except ImportError:
        return
    print(f"msgpack encode {best(lambda: msgpack.packb(REQ)):.2f} us")
    print(f"msgpack decode {best(lambda: msgpack.unpackb(data, raw=False)):.2f} us")


if __name__ == "__main__":
    main()
