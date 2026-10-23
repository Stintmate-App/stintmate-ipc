//! `PeerInfo`, `KeyFile`, the key helpers, naming and `Layout`.

use std::path::PathBuf;

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use stintmate_ipc::handshake::PeerInfo as RPeerInfo;
use stintmate_ipc::layout::{FieldType, Layout as RLayout, Scalar};
use stintmate_ipc::{KeySource, keyfile, naming};

use crate::convert::{from_json, from_value, to_value};
use crate::errors::make;

fn key_err(py: Python<'_>, e: &stintmate_ipc::KeyFileError) -> PyErr {
    make(py, "KeyFileError", (e.to_string(),))
}

fn name_err(e: stintmate_ipc::NameError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn path_obj<'py>(py: Python<'py>, p: &std::path::Path) -> PyResult<Bound<'py, PyAny>> {
    py.import("pathlib")?
        .getattr("Path")?
        .call1((p.to_string_lossy().into_owned(),))
}

// --------------------------------------------------------------------------- PeerInfo

/// Who a peer says it is (`auth.peer` / `welcome.peer`).
#[pyclass(module = "stintmate_ipc", get_all, set_all, eq)]
pub struct PeerInfo {
    /// The app id as in endpoint names.
    pub app: String,
    /// A display name.
    pub name: String,
    /// The program's version.
    pub version: String,
    /// Which process of the app.
    pub process: Option<String>,
    /// App-specific facts.
    pub meta: Py<PyAny>,
}

impl PartialEq for PeerInfo {
    fn eq(&self, o: &Self) -> bool {
        Python::attach(|py| {
            self.app == o.app
                && self.name == o.name
                && self.version == o.version
                && self.process == o.process
                && self.meta.bind(py).eq(o.meta.bind(py)).unwrap_or(false)
        })
    }
}

#[pymethods]
impl PeerInfo {
    #[new]
    #[pyo3(signature = (app, name="".to_owned(), version="".to_owned(), process=None, meta=None))]
    fn new(
        py: Python<'_>,
        app: String,
        name: String,
        version: String,
        process: Option<String>,
        meta: Option<Bound<'_, PyAny>>,
    ) -> Self {
        Self {
            app,
            name,
            version,
            process,
            meta: meta.map_or_else(|| PyDict::new(py).into_any().unbind(), Bound::unbind),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        format!(
            "PeerInfo(app={:?}, name={:?}, version={:?}, process={:?}, meta={})",
            self.app,
            self.name,
            self.version,
            self.process,
            self.meta
                .bind(py)
                .repr()
                .map(|r| r.to_string())
                .unwrap_or_default()
        )
    }
}

impl PeerInfo {
    /// The Rust form.
    pub fn to_rust(&self, py: Python<'_>) -> PyResult<RPeerInfo> {
        let meta = self.meta.bind(py);
        let meta = if meta.is_none() || meta.len().unwrap_or(0) == 0 {
            None
        } else {
            Some(to_value(meta)?)
        };
        Ok(RPeerInfo {
            app: self.app.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
            process: self.process.clone(),
            meta,
        })
    }

    /// From the Rust form.
    pub fn from_rust(py: Python<'_>, i: &RPeerInfo) -> PyResult<Self> {
        let meta = match &i.meta {
            Some(m) => from_value(py, m)?.unbind(),
            None => PyDict::new(py).into_any().unbind(),
        };
        Ok(Self {
            app: i.app.clone(),
            name: i.name.clone(),
            version: i.version.clone(),
            process: i.process.clone(),
            meta,
        })
    }
}

// --------------------------------------------------------------------------- keys

/// A key source that re-reads the file when it changes.
#[pyclass(module = "stintmate_ipc", frozen)]
pub struct KeyFile {
    path: PathBuf,
    create: bool,
    inner: keyfile::KeyFile,
}

#[pymethods]
impl KeyFile {
    #[new]
    #[pyo3(signature = (path, create=false))]
    fn new(path: PathBuf, create: bool) -> Self {
        let inner = if create {
            keyfile::KeyFile::create(path.clone())
        } else {
            keyfile::KeyFile::open(path.clone())
        };
        Self {
            path,
            create,
            inner,
        }
    }

    /// The file.
    #[getter]
    fn path<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        path_obj(py, &self.path)
    }

    /// Whether a missing file is created.
    #[getter]
    fn create(&self) -> bool {
        self.create
    }

    fn __call__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        match self.inner.key() {
            Ok(k) => Ok(PyBytes::new(py, &k)),
            Err(e) => Err(key_err(py, &e)),
        }
    }

    fn __repr__(&self) -> String {
        format!("KeyFile({:?})", self.path.display().to_string())
    }
}

/// A key argument (`KeyFile` or 32 bytes) as a core key source.
pub fn key_source(key: &Bound<'_, PyAny>) -> PyResult<KeySource> {
    if let Ok(kf) = key.cast::<KeyFile>() {
        let kf = kf.get();
        return Ok(KeySource::File(if kf.create {
            keyfile::KeyFile::create(kf.path.clone())
        } else {
            keyfile::KeyFile::open(kf.path.clone())
        }));
    }
    if let Ok(b) = key.cast::<PyBytes>() {
        let k: keyfile::Key = b
            .as_bytes()
            .try_into()
            .map_err(|_| PyValueError::new_err("a key is 32 bytes"))?;
        return Ok(KeySource::Bytes(k));
    }
    Err(PyTypeError::new_err("key must be a KeyFile or 32 bytes"))
}

/// `<workspace>\ipc\key`.
#[pyfunction]
#[pyo3(signature = (workspace=None))]
pub fn default_key_path<'py>(
    py: Python<'py>,
    workspace: Option<PathBuf>,
) -> PyResult<Bound<'py, PyAny>> {
    match keyfile::default_key_path(workspace.as_deref()) {
        Ok(p) => path_obj(py, &p),
        Err(e) => Err(key_err(py, &e)),
    }
}

/// Reads the key, creating the file first if it does not exist (atomic, no replace).
#[pyfunction]
pub fn ensure_key<'py>(py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyBytes>> {
    match py.detach(|| keyfile::ensure_key(&path)) {
        Ok(k) => Ok(PyBytes::new(py, &k)),
        Err(e) => Err(key_err(py, &e)),
    }
}

/// Reads the key of a file.
#[pyfunction]
pub fn read_key<'py>(py: Python<'py>, path: PathBuf) -> PyResult<Bound<'py, PyBytes>> {
    match py.detach(|| keyfile::read_key(&path)) {
        Ok(k) => Ok(PyBytes::new(py, &k)),
        Err(e) => Err(key_err(py, &e)),
    }
}

/// The 32 key bytes from the file's contents.
#[pyfunction]
pub fn parse_key<'py>(py: Python<'py>, text: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    match keyfile::parse_key(text) {
        Ok(k) => Ok(PyBytes::new(py, &k)),
        Err(e) => Err(key_err(py, &e)),
    }
}

/// The file's contents for a key (64 lowercase hex characters and a line end).
#[pyfunction]
pub fn format_key<'py>(py: Python<'py>, key: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let k: keyfile::Key = key
        .try_into()
        .map_err(|_| PyValueError::new_err("a key is 32 bytes"))?;
    Ok(PyBytes::new(py, &keyfile::format_key(&k)))
}

// --------------------------------------------------------------------------- naming

/// The `<user>` part of endpoint names (the Windows user name, ASCII-lowercased).
#[pyfunction]
#[pyo3(signature = (user=None))]
pub fn pipe_user(user: Option<&str>) -> PyResult<String> {
    match user {
        Some(u) => Ok(naming::ascii_lower(u)),
        None => naming::pipe_user().map_err(name_err),
    }
}

/// `stintmate-<app>[-<process>]-<user>`.
#[pyfunction]
#[pyo3(signature = (app, process=None, user=None))]
pub fn short_name(app: &str, process: Option<&str>, user: Option<&str>) -> PyResult<String> {
    match user {
        Some(u) => naming::short_name_for(app, process, &naming::ascii_lower(u)),
        None => naming::short_name(app, process),
    }
    .map_err(name_err)
}

/// `\\.\pipe\stintmate-<app>[-<process>]-<user>`.
#[pyfunction]
#[pyo3(signature = (app, process=None, user=None))]
pub fn pipe_path(app: &str, process: Option<&str>, user: Option<&str>) -> PyResult<String> {
    match user {
        Some(u) => naming::pipe_path_for(app, process, &naming::ascii_lower(u)),
        None => naming::pipe_path(app, process),
    }
    .map_err(name_err)
}

/// The Stintmate workspace (SPEC 4.3), or `None`.
#[pyfunction]
pub fn find_workspace(py: Python<'_>) -> PyResult<Option<Bound<'_, PyAny>>> {
    naming::find_workspace()
        .map(|p| path_obj(py, &p))
        .transpose()
}

/// `<workspace>\ipc\endpoints`.
#[pyfunction]
pub fn registry_dir(py: Python<'_>, workspace: PathBuf) -> PyResult<Bound<'_, PyAny>> {
    path_obj(py, &naming::registry_dir(&workspace))
}

/// The registry's entries as JSON-shaped dicts (`stintmate_ipc.list_endpoints` wraps them).
#[pyfunction]
pub fn _list_endpoints<'py>(
    py: Python<'py>,
    directory: PathBuf,
    alive_only: bool,
    prune: bool,
) -> PyResult<Bound<'py, PyList>> {
    let entries = py.detach(|| naming::list_endpoints(&directory, alive_only, prune));
    let items = entries
        .iter()
        .map(|e| from_json(py, &e.to_json()))
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, items)
}

/// Whether a pipe exists.
#[pyfunction]
pub fn _pipe_exists(pipe: &str) -> bool {
    stintmate_ipc::security::pipe_exists(pipe)
}

// --------------------------------------------------------------------------- Layout

fn scalar_to_py<'py>(py: Python<'py>, s: Scalar) -> PyResult<Bound<'py, PyAny>> {
    Ok(match s {
        Scalar::U8(v) => v.into_pyobject(py)?.into_any(),
        Scalar::I8(v) => v.into_pyobject(py)?.into_any(),
        Scalar::U16(v) => v.into_pyobject(py)?.into_any(),
        Scalar::I16(v) => v.into_pyobject(py)?.into_any(),
        Scalar::U32(v) => v.into_pyobject(py)?.into_any(),
        Scalar::I32(v) => v.into_pyobject(py)?.into_any(),
        Scalar::U64(v) => v.into_pyobject(py)?.into_any(),
        Scalar::I64(v) => v.into_pyobject(py)?.into_any(),
        Scalar::F32(v) => f64::from(v).into_pyobject(py)?.into_any(),
        Scalar::F64(v) => v.into_pyobject(py)?.into_any(),
    })
}

fn scalar_from_py(ty: FieldType, v: &Bound<'_, PyAny>) -> PyResult<Scalar> {
    fn int<T: TryFrom<i128>>(v: &Bound<'_, PyAny>, ty: FieldType) -> PyResult<T> {
        let i: i128 = v.extract()?;
        T::try_from(i).map_err(|_| PyValueError::new_err(format!("{i} does not fit {}", ty.name())))
    }
    Ok(match ty {
        FieldType::U8 => Scalar::U8(int(v, ty)?),
        FieldType::I8 => Scalar::I8(int(v, ty)?),
        FieldType::U16 => Scalar::U16(int(v, ty)?),
        FieldType::I16 => Scalar::I16(int(v, ty)?),
        FieldType::U32 => Scalar::U32(int(v, ty)?),
        FieldType::I32 => Scalar::I32(int(v, ty)?),
        FieldType::U64 => Scalar::U64(int(v, ty)?),
        FieldType::I64 => Scalar::I64(int(v, ty)?),
        FieldType::F32 => Scalar::F32(v.extract::<f64>()? as f32),
        FieldType::F64 => Scalar::F64(v.extract::<f64>()?),
    })
}

/// A declared fixed layout of stream samples (SPEC 9.3).
#[pyclass(module = "stintmate_ipc", frozen)]
pub struct Layout {
    pub(crate) inner: RLayout,
    st: std::sync::OnceLock<Py<PyAny>>,
}

impl Layout {
    /// Wraps a core layout.
    pub fn from_rust(inner: RLayout) -> Self {
        Self {
            inner,
            st: std::sync::OnceLock::new(),
        }
    }

    fn unpack_scalars(&self, body: &[u8]) -> PyResult<Vec<Scalar>> {
        let mut out = Vec::with_capacity(self.inner.value_count());
        self.inner
            .unpack(body, &mut out)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(out)
    }

    /// The values of a body by field name (arrays as tuples).
    pub fn unpack_dict_obj<'py>(
        &self,
        py: Python<'py>,
        body: &[u8],
    ) -> PyResult<Bound<'py, PyDict>> {
        let flat = self.unpack_scalars(body)?;
        let d = PyDict::new(py);
        let mut i = 0;
        for f in self.inner.fields() {
            if f.count == 1 {
                d.set_item(&f.name, scalar_to_py(py, flat[i])?)?;
            } else {
                let items = flat[i..i + f.count]
                    .iter()
                    .map(|s| scalar_to_py(py, *s))
                    .collect::<PyResult<Vec<_>>>()?;
                d.set_item(&f.name, PyTuple::new(py, items)?)?;
            }
            i += f.count;
        }
        Ok(d)
    }

    /// The flat values of a body.
    pub fn unpack_tuple<'py>(&self, py: Python<'py>, body: &[u8]) -> PyResult<Bound<'py, PyTuple>> {
        let items = self
            .unpack_scalars(body)?
            .into_iter()
            .map(|s| scalar_to_py(py, s))
            .collect::<PyResult<Vec<_>>>()?;
        PyTuple::new(py, items)
    }
}

#[pymethods]
impl Layout {
    #[new]
    #[pyo3(signature = (fields, name=None))]
    fn new(fields: &Bound<'_, PyAny>, name: Option<String>) -> PyResult<Self> {
        let mut parsed: Vec<(String, FieldType, usize)> = Vec::new();
        for f in fields.try_iter()? {
            let f = f?;
            let n = f.len()?;
            if !(2..=3).contains(&n) {
                return Err(PyValueError::new_err(format!(
                    "field {f}: (name, type) or (name, type, count)"
                )));
            }
            let fname: String = f.get_item(0)?.extract()?;
            let tname: String = f.get_item(1)?.extract()?;
            let ty = FieldType::parse(&tname).ok_or_else(|| {
                PyValueError::new_err(format!("field {fname:?}: unknown type {tname:?}"))
            })?;
            let count: usize = if n == 3 { f.get_item(2)?.extract()? } else { 1 };
            parsed.push((fname, ty, count));
        }
        let inner = RLayout::new(parsed, name.as_deref())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self::from_rust(inner))
    }

    /// The `struct.Struct` of a body (little-endian, no padding), as in 0.1.4.
    #[getter]
    fn r#struct(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(s) = self.st.get() {
            return Ok(s.clone_ref(py));
        }
        let mut fmt = String::from("<");
        for f in self.inner.fields() {
            if f.count > 1 {
                fmt.push_str(&f.count.to_string());
            }
            fmt.push(match f.ty.name() {
                "u8" => 'B',
                "i8" => 'b',
                "u16" => 'H',
                "i16" => 'h',
                "u32" => 'I',
                "i32" => 'i',
                "u64" => 'Q',
                "i64" => 'q',
                "f32" => 'f',
                _ => 'd',
            });
        }
        let s = py.import("struct")?.getattr("Struct")?.call1((fmt,))?;
        Ok(self.st.get_or_init(|| s.unbind()).clone_ref(py))
    }

    /// The layout's name.
    #[getter]
    fn name(&self) -> Option<String> {
        self.inner.name().map(str::to_owned)
    }

    /// Bytes in a packed body.
    #[getter]
    fn size(&self) -> usize {
        self.inner.size()
    }

    /// `(name, type, count)` per field.
    #[getter]
    fn fields(&self) -> Vec<(String, &'static str, usize)> {
        self.inner
            .fields()
            .iter()
            .map(|f| (f.name.clone(), f.ty.name(), f.count))
            .collect()
    }

    /// The field names.
    #[getter]
    fn names(&self) -> Vec<String> {
        self.inner.fields().iter().map(|f| f.name.clone()).collect()
    }

    /// Packs flat values (an array field takes `count` consecutive values).
    #[pyo3(signature = (*values))]
    fn pack<'py>(
        &self,
        py: Python<'py>,
        values: &Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut flat = Vec::with_capacity(values.len());
        let mut types = self
            .inner
            .fields()
            .iter()
            .flat_map(|f| std::iter::repeat_n(f.ty, f.count));
        if values.len() != self.inner.value_count() {
            return Err(PyValueError::new_err(format!(
                "expected {} values, got {}",
                self.inner.value_count(),
                values.len()
            )));
        }
        for v in values.iter() {
            flat.push(scalar_from_py(types.next().expect("counted"), &v)?);
        }
        let mut out = Vec::with_capacity(self.inner.size());
        self.inner
            .pack(&flat, &mut out)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyBytes::new(py, &out))
    }

    /// Packs a mapping of field name to value (arrays as sequences).
    fn pack_dict<'py>(
        &self,
        py: Python<'py>,
        values: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut flat = Vec::with_capacity(self.inner.value_count());
        for f in self.inner.fields() {
            let v = values.get_item(&f.name)?;
            if f.count == 1 {
                flat.push(scalar_from_py(f.ty, &v)?);
            } else {
                if v.len()? != f.count {
                    return Err(PyValueError::new_err(format!(
                        "{}: expected {} values",
                        f.name, f.count
                    )));
                }
                for x in v.try_iter()? {
                    flat.push(scalar_from_py(f.ty, &x?)?);
                }
            }
        }
        let mut out = Vec::with_capacity(self.inner.size());
        self.inner
            .pack(&flat, &mut out)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyBytes::new(py, &out))
    }

    /// The flat values of a body.
    #[pyo3(name = "unpack")]
    fn unpack_py<'py>(&self, py: Python<'py>, body: &[u8]) -> PyResult<Bound<'py, PyTuple>> {
        self.unpack_tuple(py, body)
    }

    /// The values of a body by field name.
    #[pyo3(name = "unpack_dict")]
    fn unpack_dict_py<'py>(&self, py: Python<'py>, body: &[u8]) -> PyResult<Bound<'py, PyDict>> {
        self.unpack_dict_obj(py, body)
    }

    /// The wire form (a dict).
    fn to_wire<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        from_value(py, &self.inner.to_value())
    }

    /// From the wire form.
    #[staticmethod]
    fn from_wire(d: &Bound<'_, PyAny>) -> PyResult<Self> {
        let inner =
            RLayout::from_value(&to_value(d)?).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self::from_rust(inner))
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other
            .cast::<Layout>()
            .is_ok_and(|o| o.get().inner.fields() == self.inner.fields())
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for f in self.inner.fields() {
            (&f.name, f.ty, f.count).hash(&mut h);
        }
        h.finish()
    }

    fn __repr__(&self) -> String {
        format!("Layout({:?}, name={:?})", self.fields(), self.inner.name())
    }
}
