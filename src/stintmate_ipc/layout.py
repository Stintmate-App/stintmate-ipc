"""Declared fixed layouts of stream samples (SPEC.md section 9.3).

A layout is a list of named fields, packed little-endian with no padding, in order::

    Layout([("speed_kmh", "f32"), ("gear", "i8"), ("flags", "u8"), ("rpm", "u16"),
            ("tyre_temp_c", "f32", 4)], name="telemetry")
"""

from __future__ import annotations

import re
import struct
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

TYPES = {"u8": "B", "i8": "b", "u16": "H", "i16": "h", "u32": "I", "i32": "i",
         "u64": "Q", "i64": "q", "f32": "f", "f64": "d"}
FIELD_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
MAX_FIELDS = 1024
MAX_SIZE = 65536


class Layout:
    def __init__(self, fields: Iterable[Sequence[Any]], name: str | None = None) -> None:
        self.name = name
        self.fields: list[tuple[str, str, int]] = []
        fmt = "<"
        seen: set[str] = set()
        for f in fields:
            if len(f) not in (2, 3):
                raise ValueError(f"field {f!r}: (name, type) or (name, type, count)")
            fname, ftype = f[0], f[1]
            count = int(f[2]) if len(f) == 3 else 1
            if not isinstance(fname, str) or not FIELD_RE.match(fname):
                raise ValueError(f"invalid field name {fname!r}")
            if fname in seen:
                raise ValueError(f"duplicate field {fname!r}")
            if ftype not in TYPES:
                raise ValueError(f"field {fname!r}: unknown type {ftype!r}")
            if not 1 <= count <= 4096:
                raise ValueError(f"field {fname!r}: count must be 1..4096")
            seen.add(fname)
            self.fields.append((fname, ftype, count))
            fmt += (str(count) if count > 1 else "") + TYPES[ftype]
        if not self.fields:
            raise ValueError("a layout needs at least one field")
        if len(self.fields) > MAX_FIELDS:
            raise ValueError("too many fields")
        self.struct = struct.Struct(fmt)
        self.size = self.struct.size
        if self.size > MAX_SIZE:
            raise ValueError(f"layout of {self.size} bytes exceeds {MAX_SIZE}")
        self.names = [f[0] for f in self.fields]

    def pack(self, *values: Any) -> bytes:
        """Pack flat values (an array field takes ``count`` consecutive values)."""
        return self.struct.pack(*values)

    def pack_dict(self, values: Mapping[str, Any]) -> bytes:
        flat: list[Any] = []
        for fname, _t, count in self.fields:
            v = values[fname]
            if count == 1:
                flat.append(v)
            else:
                if len(v) != count:
                    raise ValueError(f"{fname}: expected {count} values")
                flat.extend(v)
        return self.struct.pack(*flat)

    def unpack(self, body: bytes) -> tuple[Any, ...]:
        return self.struct.unpack(body)

    def unpack_dict(self, body: bytes) -> dict[str, Any]:
        flat = self.struct.unpack(body)
        out: dict[str, Any] = {}
        i = 0
        for fname, _t, count in self.fields:
            if count == 1:
                out[fname] = flat[i]
            else:
                out[fname] = tuple(flat[i:i + count])
            i += count
        return out

    def to_wire(self) -> dict[str, Any]:
        d: dict[str, Any] = {"size": self.size,
                             "fields": [[n, t] if c == 1 else [n, t, c]
                                        for n, t, c in self.fields]}
        if self.name:
            d["name"] = self.name
        return d

    @classmethod
    def from_wire(cls, d: Any) -> Layout:
        if not isinstance(d, dict) or not isinstance(d.get("fields"), list):
            raise ValueError("layout must be a map with 'fields'")
        name = d.get("name")
        lay = cls([tuple(f) for f in d["fields"]], name=name if isinstance(name, str) else None)
        if "size" in d and d["size"] != lay.size:
            raise ValueError(f"layout size {d['size']} != sum of its fields {lay.size}")
        return lay

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Layout) and self.fields == other.fields

    def __hash__(self) -> int:
        return hash(tuple(self.fields))

    def __repr__(self) -> str:
        return f"Layout({self.fields!r}, name={self.name!r})"
