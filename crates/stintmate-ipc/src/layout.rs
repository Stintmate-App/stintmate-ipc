//! Declared fixed layouts of stream samples (SPEC 9.3).
//!
//! A layout is a list of named fields packed little-endian, in order, with no padding:
//!
//! ```
//! use stintmate_ipc::layout::{FieldType, Layout, Scalar};
//!
//! let layout = Layout::new(
//!     [("speed_kmh", FieldType::F32, 1), ("gear", FieldType::I8, 1), ("tyre_c", FieldType::F32, 4)],
//!     Some("telemetry"),
//! ).unwrap();
//! assert_eq!(layout.size(), 21);
//! let mut body = Vec::new();
//! layout.pack(&[Scalar::F32(212.5), Scalar::I8(5),
//!               Scalar::F32(85.0), Scalar::F32(85.5), Scalar::F32(86.0), Scalar::F32(86.5)],
//!             &mut body).unwrap();
//! assert_eq!(body.len(), 21);
//! ```

use crate::codec::{self, Value};
use crate::error::LayoutError;

/// Most fields in a layout.
pub const MAX_FIELDS: usize = 1024;
/// Largest layout, in bytes.
pub const MAX_SIZE: usize = 65536;
/// Largest `count` of an array field.
pub const MAX_COUNT: usize = 4096;

/// The type of a layout field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FieldType {
    /// Unsigned 8-bit.
    U8,
    /// Signed 8-bit.
    I8,
    /// Unsigned 16-bit.
    U16,
    /// Signed 16-bit.
    I16,
    /// Unsigned 32-bit.
    U32,
    /// Signed 32-bit.
    I32,
    /// Unsigned 64-bit.
    U64,
    /// Signed 64-bit.
    I64,
    /// IEEE 754 binary32.
    F32,
    /// IEEE 754 binary64.
    F64,
}

impl FieldType {
    /// Size in bytes.
    pub const fn size(self) -> usize {
        match self {
            Self::U8 | Self::I8 => 1,
            Self::U16 | Self::I16 => 2,
            Self::U32 | Self::I32 | Self::F32 => 4,
            Self::U64 | Self::I64 | Self::F64 => 8,
        }
    }

    /// The name used on the wire (`u8`, `f32`, ...).
    pub const fn name(self) -> &'static str {
        match self {
            Self::U8 => "u8",
            Self::I8 => "i8",
            Self::U16 => "u16",
            Self::I16 => "i16",
            Self::U32 => "u32",
            Self::I32 => "i32",
            Self::U64 => "u64",
            Self::I64 => "i64",
            Self::F32 => "f32",
            Self::F64 => "f64",
        }
    }

    /// Parses a wire name.
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "u8" => Self::U8,
            "i8" => Self::I8,
            "u16" => Self::U16,
            "i16" => Self::I16,
            "u32" => Self::U32,
            "i32" => Self::I32,
            "u64" => Self::U64,
            "i64" => Self::I64,
            "f32" => Self::F32,
            "f64" => Self::F64,
            _ => return None,
        })
    }
}

/// One value of a field.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Scalar {
    /// A `u8`.
    U8(u8),
    /// An `i8`.
    I8(i8),
    /// A `u16`.
    U16(u16),
    /// An `i16`.
    I16(i16),
    /// A `u32`.
    U32(u32),
    /// An `i32`.
    I32(i32),
    /// A `u64`.
    U64(u64),
    /// An `i64`.
    I64(i64),
    /// An `f32`.
    F32(f32),
    /// An `f64`.
    F64(f64),
}

impl Scalar {
    /// The field type this value belongs to.
    pub const fn field_type(self) -> FieldType {
        match self {
            Self::U8(_) => FieldType::U8,
            Self::I8(_) => FieldType::I8,
            Self::U16(_) => FieldType::U16,
            Self::I16(_) => FieldType::I16,
            Self::U32(_) => FieldType::U32,
            Self::I32(_) => FieldType::I32,
            Self::U64(_) => FieldType::U64,
            Self::I64(_) => FieldType::I64,
            Self::F32(_) => FieldType::F32,
            Self::F64(_) => FieldType::F64,
        }
    }

    fn to_value(self) -> Value {
        match self {
            Self::U8(v) => Value::from(v),
            Self::I8(v) => Value::from(v),
            Self::U16(v) => Value::from(v),
            Self::I16(v) => Value::from(v),
            Self::U32(v) => Value::from(v),
            Self::I32(v) => Value::from(v),
            Self::U64(v) => Value::from(v),
            Self::I64(v) => Value::from(v),
            Self::F32(v) => Value::F64(f64::from(v)),
            Self::F64(v) => Value::F64(v),
        }
    }

    fn from_value(ty: FieldType, v: &Value) -> Option<Self> {
        let int = |lo: i128, hi: i128| -> Option<i128> {
            let n = v
                .as_i64()
                .map(i128::from)
                .or_else(|| v.as_u64().map(i128::from))?;
            (lo..=hi).contains(&n).then_some(n)
        };
        Some(match ty {
            FieldType::U8 => Self::U8(int(0, u8::MAX.into())? as u8),
            FieldType::I8 => Self::I8(int(i8::MIN.into(), i8::MAX.into())? as i8),
            FieldType::U16 => Self::U16(int(0, u16::MAX.into())? as u16),
            FieldType::I16 => Self::I16(int(i16::MIN.into(), i16::MAX.into())? as i16),
            FieldType::U32 => Self::U32(int(0, u32::MAX.into())? as u32),
            FieldType::I32 => Self::I32(int(i32::MIN.into(), i32::MAX.into())? as i32),
            FieldType::U64 => Self::U64(int(0, u64::MAX.into())? as u64),
            FieldType::I64 => Self::I64(int(i64::MIN.into(), i64::MAX.into())? as i64),
            FieldType::F32 => Self::F32(v.as_f64()? as f32),
            FieldType::F64 => Self::F64(v.as_f64()?),
        })
    }
}

/// A named field: `count` consecutive values of one type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// The name, matching `^[a-z][a-z0-9_]{0,63}$`, unique in the layout.
    pub name: String,
    /// The value type.
    pub ty: FieldType,
    /// 1 for a scalar, 2-4096 for an array.
    pub count: usize,
}

/// A validated layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    name: Option<String>,
    fields: Vec<Field>,
    size: usize,
    values: usize,
}

fn valid_field_name(name: &str) -> bool {
    let b = name.as_bytes();
    (1..=64).contains(&b.len())
        && b[0].is_ascii_lowercase()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_')
}

impl Layout {
    /// Builds and validates a layout from `(name, type, count)` fields.
    pub fn new<I, S>(fields: I, name: Option<&str>) -> Result<Self, LayoutError>
    where
        I: IntoIterator<Item = (S, FieldType, usize)>,
        S: Into<String>,
    {
        let mut out: Vec<Field> = Vec::new();
        let mut size = 0usize;
        let mut values = 0usize;
        for (fname, ty, count) in fields {
            let fname = fname.into();
            let invalid = |m: String| LayoutError::Invalid(m);
            if !valid_field_name(&fname) {
                return Err(invalid(format!("invalid field name {fname:?}")));
            }
            if out.iter().any(|f| f.name == fname) {
                return Err(invalid(format!("duplicate field {fname:?}")));
            }
            if !(1..=MAX_COUNT).contains(&count) {
                return Err(invalid(format!(
                    "field {fname:?}: count must be 1..{MAX_COUNT}"
                )));
            }
            size += ty.size() * count;
            values += count;
            if out.len() >= MAX_FIELDS {
                return Err(invalid("too many fields".into()));
            }
            if size > MAX_SIZE {
                return Err(invalid(format!("layout exceeds {MAX_SIZE} bytes")));
            }
            out.push(Field {
                name: fname,
                ty,
                count,
            });
        }
        if out.is_empty() {
            return Err(LayoutError::Invalid(
                "a layout needs at least one field".into(),
            ));
        }
        Ok(Self {
            name: name.map(str::to_owned),
            fields: out,
            size,
            values,
        })
    }

    /// The informative name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// The fields, in order.
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// The size of a body, in bytes.
    pub fn size(&self) -> usize {
        self.size
    }

    /// How many scalar values a body holds (arrays counted per element).
    pub fn value_count(&self) -> usize {
        self.values
    }

    /// Packs flat values (an array field takes `count` consecutive values), appending to `out`.
    pub fn pack(&self, values: &[Scalar], out: &mut Vec<u8>) -> Result<(), LayoutError> {
        if values.len() != self.values {
            return Err(LayoutError::Mismatch(format!(
                "expected {} values, got {}",
                self.values,
                values.len()
            )));
        }
        out.reserve(self.size);
        let mut it = values.iter();
        for f in &self.fields {
            for _ in 0..f.count {
                let v = *it.next().expect("count checked");
                match (f.ty, v) {
                    (FieldType::U8, Scalar::U8(x)) => out.push(x),
                    (FieldType::I8, Scalar::I8(x)) => out.push(x as u8),
                    (FieldType::U16, Scalar::U16(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (FieldType::I16, Scalar::I16(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (FieldType::U32, Scalar::U32(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (FieldType::I32, Scalar::I32(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (FieldType::U64, Scalar::U64(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (FieldType::I64, Scalar::I64(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (FieldType::F32, Scalar::F32(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (FieldType::F64, Scalar::F64(x)) => out.extend_from_slice(&x.to_le_bytes()),
                    (want, got) => {
                        return Err(LayoutError::Mismatch(format!(
                            "field {:?} is {}, got a {} value",
                            f.name,
                            want.name(),
                            got.field_type().name()
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Unpacks a body into flat values (the buffer is cleared first, so it can be reused).
    pub fn unpack(&self, body: &[u8], out: &mut Vec<Scalar>) -> Result<(), LayoutError> {
        if body.len() != self.size {
            return Err(LayoutError::Mismatch(format!(
                "body of {} bytes, layout of {}",
                body.len(),
                self.size
            )));
        }
        out.clear();
        out.reserve(self.values);
        let mut pos = 0;
        for f in &self.fields {
            for _ in 0..f.count {
                let b = &body[pos..pos + f.ty.size()];
                pos += f.ty.size();
                out.push(match f.ty {
                    FieldType::U8 => Scalar::U8(b[0]),
                    FieldType::I8 => Scalar::I8(b[0] as i8),
                    FieldType::U16 => Scalar::U16(u16::from_le_bytes([b[0], b[1]])),
                    FieldType::I16 => Scalar::I16(i16::from_le_bytes([b[0], b[1]])),
                    FieldType::U32 => Scalar::U32(u32::from_le_bytes(b.try_into().unwrap())),
                    FieldType::I32 => Scalar::I32(i32::from_le_bytes(b.try_into().unwrap())),
                    FieldType::U64 => Scalar::U64(u64::from_le_bytes(b.try_into().unwrap())),
                    FieldType::I64 => Scalar::I64(i64::from_le_bytes(b.try_into().unwrap())),
                    FieldType::F32 => Scalar::F32(f32::from_le_bytes(b.try_into().unwrap())),
                    FieldType::F64 => Scalar::F64(f64::from_le_bytes(b.try_into().unwrap())),
                });
            }
        }
        Ok(())
    }

    /// Packs a map `{field: value}` (an array field takes an array), appending to `out`.
    pub fn pack_value(&self, values: &Value, out: &mut Vec<u8>) -> Result<(), LayoutError> {
        let mut flat = Vec::with_capacity(self.values);
        for f in &self.fields {
            let v = codec::get(values, &f.name)
                .ok_or_else(|| LayoutError::Mismatch(format!("missing field {:?}", f.name)))?;
            let bad = || LayoutError::Mismatch(format!("field {:?}: bad value", f.name));
            if f.count == 1 {
                flat.push(Scalar::from_value(f.ty, v).ok_or_else(bad)?);
            } else {
                let items = v.as_array().filter(|a| a.len() == f.count).ok_or_else(|| {
                    LayoutError::Mismatch(format!("{}: expected {} values", f.name, f.count))
                })?;
                for item in items {
                    flat.push(Scalar::from_value(f.ty, item).ok_or_else(bad)?);
                }
            }
        }
        self.pack(&flat, out)
    }

    /// Unpacks a body into a map `{field: value}` (arrays for `count` > 1; floats as float64).
    pub fn unpack_value(&self, body: &[u8]) -> Result<Value, LayoutError> {
        let mut flat = Vec::new();
        self.unpack(body, &mut flat)?;
        let mut it = flat.into_iter();
        let mut entries = Vec::with_capacity(self.fields.len());
        for f in &self.fields {
            let v = if f.count == 1 {
                it.next().expect("count checked").to_value()
            } else {
                Value::Array(it.by_ref().take(f.count).map(Scalar::to_value).collect())
            };
            entries.push((Value::from(f.name.as_str()), v));
        }
        Ok(Value::Map(entries))
    }

    /// The wire form: `{"size", "fields": [[name, type] | [name, type, count]], "name"?}`.
    pub fn to_value(&self) -> Value {
        let fields = self
            .fields
            .iter()
            .map(|f| {
                let mut a = vec![Value::from(f.name.as_str()), Value::from(f.ty.name())];
                if f.count != 1 {
                    a.push(Value::from(f.count as u64));
                }
                Value::Array(a)
            })
            .collect();
        let mut pairs = vec![
            ("size", Value::from(self.size as u64)),
            ("fields", Value::Array(fields)),
        ];
        if let Some(n) = &self.name {
            pairs.push(("name", Value::from(n.as_str())));
        }
        codec::map(pairs)
    }

    /// Parses and validates the wire form. `size`, when present, must equal the sum of the fields;
    /// unknown keys are ignored.
    pub fn from_value(v: &Value) -> Result<Self, LayoutError> {
        let invalid = |m: &str| LayoutError::Invalid(m.to_owned());
        let fields = codec::get(v, "fields")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("layout must be a map with 'fields'"))?;
        let mut parsed = Vec::with_capacity(fields.len().min(MAX_FIELDS));
        for f in fields {
            let a = f
                .as_array()
                .filter(|a| (2..=3).contains(&a.len()))
                .ok_or_else(|| invalid("a field is [name, type] or [name, type, count]"))?;
            let name = a[0]
                .as_str()
                .ok_or_else(|| invalid("field name must be a string"))?;
            let ty = a[1]
                .as_str()
                .and_then(FieldType::parse)
                .ok_or_else(|| invalid("unknown field type"))?;
            let count = match a.get(2) {
                None => 1,
                Some(c) => c
                    .as_u64()
                    .and_then(|c| usize::try_from(c).ok())
                    .ok_or_else(|| invalid("field count must be an unsigned integer"))?,
            };
            parsed.push((name.to_owned(), ty, count));
        }
        let name = codec::get(v, "name").and_then(Value::as_str);
        let layout = Self::new(parsed, name)?;
        if let Some(size) = codec::get(v, "size")
            && size.as_u64() != Some(layout.size as u64)
        {
            return Err(LayoutError::Invalid(format!(
                "layout size {size} != sum of its fields {}",
                layout.size
            )));
        }
        Ok(layout)
    }
}
