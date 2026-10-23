//! Python objects <-> MessagePack values (SPEC 6.1): tuples as arrays, `bytes` as bin;
//! non-string map keys and integers outside 64 bits raise `TypeError` before anything is sent.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};
use rmpv::Value;

/// Deepest nesting accepted (the codec's limit).
const MAX_DEPTH: usize = 64;

fn type_name(obj: &Bound<'_, PyAny>) -> String {
    obj.get_type()
        .name()
        .map(|n| n.to_string())
        .unwrap_or_default()
}

/// A Python object as a value.
pub fn to_value(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    to_value_depth(obj, 0)
}

fn to_value_depth(obj: &Bound<'_, PyAny>, depth: usize) -> PyResult<Value> {
    if depth > MAX_DEPTH {
        return Err(PyValueError::new_err("value nested too deeply"));
    }
    if obj.is_none() {
        return Ok(Value::Nil);
    }
    if obj.is_instance_of::<PyBool>() {
        return Ok(Value::Boolean(obj.is_truthy()?));
    }
    if obj.is_instance_of::<PyInt>() {
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(Value::from(i));
        }
        if let Ok(u) = obj.extract::<u64>() {
            return Ok(Value::from(u));
        }
        return Err(PyTypeError::new_err(
            "integers outside the 64-bit range cannot be sent (SPEC 6.1)",
        ));
    }
    if obj.is_instance_of::<PyFloat>() {
        return Ok(Value::F64(obj.extract::<f64>()?));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        return Ok(Value::from(s.to_str()?));
    }
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(Value::Binary(b.as_bytes().to_vec()));
    }
    if let Ok(b) = obj.cast::<PyByteArray>() {
        return Ok(Value::Binary(b.to_vec()));
    }
    if let Ok(l) = obj.cast::<PyList>() {
        return l
            .iter()
            .map(|x| to_value_depth(&x, depth + 1))
            .collect::<PyResult<Vec<_>>>()
            .map(Value::Array);
    }
    if let Ok(t) = obj.cast::<PyTuple>() {
        return t
            .iter()
            .map(|x| to_value_depth(&x, depth + 1))
            .collect::<PyResult<Vec<_>>>()
            .map(Value::Array);
    }
    if let Ok(d) = obj.cast::<PyDict>() {
        let mut out = Vec::with_capacity(d.len());
        for (k, v) in d.iter() {
            let key = k.cast::<PyString>().map_err(|_| {
                PyTypeError::new_err(format!("map keys must be strings, not {}", type_name(&k)))
            })?;
            out.push((Value::from(key.to_str()?), to_value_depth(&v, depth + 1)?));
        }
        return Ok(Value::Map(out));
    }
    Err(PyTypeError::new_err(format!(
        "cannot encode {} as MessagePack",
        type_name(obj)
    )))
}

/// A value as a Python object (arrays as lists, maps as dicts, bin as bytes).
pub fn from_value<'py>(py: Python<'py>, v: &Value) -> PyResult<Bound<'py, PyAny>> {
    Ok(match v {
        Value::Nil => py.None().into_bound(py),
        Value::Boolean(b) => PyBool::new(py, *b).to_owned().into_any(),
        Value::Integer(i) => {
            if let Some(x) = i.as_i64() {
                x.into_pyobject(py)?.into_any()
            } else if let Some(x) = i.as_u64() {
                x.into_pyobject(py)?.into_any()
            } else {
                return Err(PyValueError::new_err("integer out of range"));
            }
        }
        Value::F32(f) => PyFloat::new(py, f64::from(*f)).into_any(),
        Value::F64(f) => PyFloat::new(py, *f).into_any(),
        Value::String(s) => PyString::new(py, s.as_str().unwrap_or("")).into_any(),
        Value::Binary(b) => PyBytes::new(py, b).into_any(),
        Value::Array(a) => {
            let items = a
                .iter()
                .map(|x| from_value(py, x))
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, items)?.into_any()
        }
        Value::Map(m) => {
            let d = PyDict::new(py);
            for (k, x) in m {
                let key = match k {
                    Value::String(s) => s.as_str().unwrap_or("").to_owned(),
                    _ => return Err(PyTypeError::new_err("map keys must be strings")),
                };
                d.set_item(key, from_value(py, x)?)?;
            }
            d.into_any()
        }
        Value::Ext(..) => return Err(PyTypeError::new_err("extension types are not supported")),
    })
}

/// A JSON value (registry files) as a Python object.
pub fn from_json<'py>(py: Python<'py>, v: &serde_json::Value) -> PyResult<Bound<'py, PyAny>> {
    use serde_json::Value as J;
    Ok(match v {
        J::Null => py.None().into_bound(py),
        J::Bool(b) => PyBool::new(py, *b).to_owned().into_any(),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py)?.into_any()
            } else if let Some(u) = n.as_u64() {
                u.into_pyobject(py)?.into_any()
            } else {
                PyFloat::new(py, n.as_f64().unwrap_or(0.0)).into_any()
            }
        }
        J::String(s) => PyString::new(py, s).into_any(),
        J::Array(a) => {
            let items = a
                .iter()
                .map(|x| from_json(py, x))
                .collect::<PyResult<Vec<_>>>()?;
            PyList::new(py, items)?.into_any()
        }
        J::Object(m) => {
            let d = PyDict::new(py);
            for (k, x) in m {
                d.set_item(k, from_json(py, x)?)?;
            }
            d.into_any()
        }
    })
}
