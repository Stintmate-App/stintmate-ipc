//! Direct paths between Python objects and MessagePack bytes, without the `Value` tree.
//!
//! They follow the subset of SPEC 6.1 exactly as `stintmate_ipc::codec` does: shortest forms
//! on encode; extension types, non-string or repeated keys, invalid UTF-8, nesting beyond 64,
//! truncation and trailing bytes refused on decode.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};
use stintmate_ipc::error::ProtocolError;

use crate::convert::{MAX_DEPTH, type_name};

fn bad(e: ProtocolError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn truncated() -> PyErr {
    PyValueError::new_err("invalid MessagePack: truncated value")
}

// --------------------------------------------------------------------------- encode

/// Encodes a Python object as MessagePack.
pub fn encode(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let mut out = Vec::with_capacity(128);
    write(&mut out, obj, 0)?;
    Ok(out)
}

/// A length header: `fix` is (marker base, exclusive limit) of the fix form, `w8` the 8-bit
/// marker when there is one.
fn head(out: &mut Vec<u8>, n: usize, fix: (u8, usize), w8: Option<u8>, w16: u8, w32: u8) {
    if n < fix.1 {
        out.push(fix.0 | n as u8);
    } else if let (Some(m), true) = (w8, n <= 0xff) {
        out.extend_from_slice(&[m, n as u8]);
    } else if n <= 0xffff {
        out.push(w16);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(w32);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
}

fn write_uint(out: &mut Vec<u8>, u: u64) {
    if u < 128 {
        out.push(u as u8);
    } else if u <= 0xff {
        out.extend_from_slice(&[0xcc, u as u8]);
    } else if u <= 0xffff {
        out.push(0xcd);
        out.extend_from_slice(&(u as u16).to_be_bytes());
    } else if u <= 0xffff_ffff {
        out.push(0xce);
        out.extend_from_slice(&(u as u32).to_be_bytes());
    } else {
        out.push(0xcf);
        out.extend_from_slice(&u.to_be_bytes());
    }
}

fn write_int(out: &mut Vec<u8>, i: i64) {
    if i >= 0 {
        write_uint(out, i as u64);
    } else if i >= -32 {
        out.push(i as i8 as u8);
    } else if i >= i64::from(i8::MIN) {
        out.extend_from_slice(&[0xd0, i as i8 as u8]);
    } else if i >= i64::from(i16::MIN) {
        out.push(0xd1);
        out.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= i64::from(i32::MIN) {
        out.push(0xd2);
        out.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&i.to_be_bytes());
    }
}

fn write_str(out: &mut Vec<u8>, s: &str) {
    head(out, s.len(), (0xa0, 32), Some(0xd9), 0xda, 0xdb);
    out.extend_from_slice(s.as_bytes());
}

fn write_bin(out: &mut Vec<u8>, b: &[u8]) {
    // bin has no fix form and no 0-length shortcut: c4 00 for empty
    if b.len() <= 0xff {
        out.extend_from_slice(&[0xc4, b.len() as u8]);
    } else if b.len() <= 0xffff {
        out.push(0xc5);
        out.extend_from_slice(&(b.len() as u16).to_be_bytes());
    } else {
        out.push(0xc6);
        out.extend_from_slice(&(b.len() as u32).to_be_bytes());
    }
    out.extend_from_slice(b);
}

fn write_items<'py>(
    out: &mut Vec<u8>,
    len: usize,
    items: impl Iterator<Item = Bound<'py, PyAny>>,
    depth: usize,
) -> PyResult<()> {
    head(out, len, (0x90, 16), None, 0xdc, 0xdd);
    for x in items {
        write(out, &x, depth + 1)?;
    }
    Ok(())
}

fn write(out: &mut Vec<u8>, obj: &Bound<'_, PyAny>, depth: usize) -> PyResult<()> {
    if depth > MAX_DEPTH {
        return Err(PyValueError::new_err("value nested too deeply"));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        write_str(out, s.to_str()?);
    } else if let Ok(d) = obj.cast::<PyDict>() {
        head(out, d.len(), (0x80, 16), None, 0xde, 0xdf);
        for (k, v) in d.iter() {
            let key = k.cast::<PyString>().map_err(|_| {
                PyTypeError::new_err(format!("map keys must be strings, not {}", type_name(&k)))
            })?;
            write_str(out, key.to_str()?);
            write(out, &v, depth + 1)?;
        }
    } else if obj.is_none() {
        out.push(0xc0);
    } else if obj.is_instance_of::<PyBool>() {
        out.push(if obj.is_truthy()? { 0xc3 } else { 0xc2 });
    } else if obj.is_instance_of::<PyInt>() {
        if let Ok(i) = obj.extract::<i64>() {
            write_int(out, i);
        } else if let Ok(u) = obj.extract::<u64>() {
            write_uint(out, u);
        } else {
            return Err(PyTypeError::new_err(
                "integers outside the 64-bit range cannot be sent (SPEC 6.1)",
            ));
        }
    } else if obj.is_instance_of::<PyFloat>() {
        out.push(0xcb);
        out.extend_from_slice(&obj.extract::<f64>()?.to_be_bytes());
    } else if let Ok(l) = obj.cast::<PyList>() {
        write_items(out, l.len(), l.iter(), depth)?;
    } else if let Ok(b) = obj.cast::<PyBytes>() {
        write_bin(out, b.as_bytes());
    } else if let Ok(t) = obj.cast::<PyTuple>() {
        write_items(out, t.len(), t.iter(), depth)?;
    } else if let Ok(b) = obj.cast::<PyByteArray>() {
        write_bin(out, &b.to_vec());
    } else {
        return Err(PyTypeError::new_err(format!(
            "cannot encode {} as MessagePack",
            type_name(obj)
        )));
    }
    Ok(())
}

// --------------------------------------------------------------------------- decode

/// Decodes exactly one MessagePack value into Python objects, validating as it goes.
pub fn decode<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyAny>> {
    let mut rd = Reader { data, pos: 0 };
    let v = rd.value(py, 0)?;
    if rd.pos != data.len() {
        return Err(bad(ProtocolError::TrailingData(data.len() - rd.pos)));
    }
    Ok(v)
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> PyResult<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|e| *e <= self.data.len())
            .ok_or_else(truncated)?;
        let s = &self.data[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn be<const N: usize>(&mut self) -> PyResult<[u8; N]> {
        Ok(self.take(N)?.try_into().expect("length checked"))
    }

    fn len(&mut self, width: usize) -> PyResult<usize> {
        Ok(match width {
            1 => usize::from(self.be::<1>()?[0]),
            2 => usize::from(u16::from_be_bytes(self.be()?)),
            _ => u32::from_be_bytes(self.be()?) as usize,
        })
    }

    fn str(&mut self, n: usize) -> PyResult<&'a str> {
        std::str::from_utf8(self.take(n)?).map_err(|_| bad(ProtocolError::InvalidUtf8))
    }

    fn array<'py>(
        &mut self,
        py: Python<'py>,
        n: usize,
        depth: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        if n > self.data.len() - self.pos {
            return Err(truncated());
        }
        let list = PyList::empty(py);
        for _ in 0..n {
            list.append(self.value(py, depth + 1)?)?;
        }
        Ok(list.into_any())
    }

    fn map<'py>(&mut self, py: Python<'py>, n: usize, depth: usize) -> PyResult<Bound<'py, PyAny>> {
        if n > (self.data.len() - self.pos) / 2 {
            return Err(truncated());
        }
        let d = PyDict::new(py);
        for _ in 0..n {
            let marker = self.be::<1>()?[0];
            let len = match marker {
                0xa0..=0xbf => usize::from(marker & 0x1f),
                0xd9..=0xdb => self.len(1 << (marker - 0xd9))?,
                _ => return Err(bad(ProtocolError::NonStringKey)),
            };
            let key = PyString::new(py, self.str(len)?);
            if d.contains(&key)? {
                return Err(bad(ProtocolError::DuplicateKey(key.to_string())));
            }
            d.set_item(key, self.value(py, depth + 1)?)?;
        }
        Ok(d.into_any())
    }

    fn value<'py>(&mut self, py: Python<'py>, depth: usize) -> PyResult<Bound<'py, PyAny>> {
        if depth > MAX_DEPTH {
            return Err(bad(ProtocolError::TooDeep));
        }
        let marker = self.be::<1>()?[0];
        Ok(match marker {
            0x00..=0x7f => u64::from(marker).into_pyobject(py)?.into_any(),
            0x80..=0x8f => return self.map(py, usize::from(marker & 0x0f), depth),
            0x90..=0x9f => return self.array(py, usize::from(marker & 0x0f), depth),
            0xa0..=0xbf => PyString::new(py, self.str(usize::from(marker & 0x1f))?).into_any(),
            0xc0 => py.None().into_bound(py),
            0xc1 => {
                return Err(PyValueError::new_err(
                    "invalid MessagePack: marker 0xc1 is never used",
                ));
            }
            0xc2 => PyBool::new(py, false).to_owned().into_any(),
            0xc3 => PyBool::new(py, true).to_owned().into_any(),
            0xc4..=0xc6 => {
                let n = self.len(1 << (marker - 0xc4))?;
                PyBytes::new(py, self.take(n)?).into_any()
            }
            0xc7..=0xc9 | 0xd4..=0xd8 => return Err(bad(ProtocolError::ExtensionType)),
            0xca => PyFloat::new(py, f64::from(f32::from_be_bytes(self.be()?))).into_any(),
            0xcb => PyFloat::new(py, f64::from_be_bytes(self.be()?)).into_any(),
            0xcc => u64::from(self.be::<1>()?[0]).into_pyobject(py)?.into_any(),
            0xcd => u64::from(u16::from_be_bytes(self.be()?))
                .into_pyobject(py)?
                .into_any(),
            0xce => u64::from(u32::from_be_bytes(self.be()?))
                .into_pyobject(py)?
                .into_any(),
            0xcf => u64::from_be_bytes(self.be()?).into_pyobject(py)?.into_any(),
            0xd0 => i64::from(self.be::<1>()?[0] as i8)
                .into_pyobject(py)?
                .into_any(),
            0xd1 => i64::from(i16::from_be_bytes(self.be()?))
                .into_pyobject(py)?
                .into_any(),
            0xd2 => i64::from(i32::from_be_bytes(self.be()?))
                .into_pyobject(py)?
                .into_any(),
            0xd3 => i64::from_be_bytes(self.be()?).into_pyobject(py)?.into_any(),
            0xd9..=0xdb => {
                let n = self.len(1 << (marker - 0xd9))?;
                PyString::new(py, self.str(n)?).into_any()
            }
            0xdc => {
                let n = self.len(2)?;
                return self.array(py, n, depth);
            }
            0xdd => {
                let n = self.len(4)?;
                return self.array(py, n, depth);
            }
            0xde => {
                let n = self.len(2)?;
                return self.map(py, n, depth);
            }
            0xdf => {
                let n = self.len(4)?;
                return self.map(py, n, depth);
            }
            0xe0..=0xff => i64::from(marker as i8).into_pyobject(py)?.into_any(),
        })
    }
}
