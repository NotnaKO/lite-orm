#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};
use std::{borrow::Cow, fmt};

use rusqlite::types::{FromSql, ToSqlOutput};
use rusqlite::ToSql;

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub struct ObjectId(i64);

impl ToSql for ObjectId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        self.0.to_sql()
    }
}

impl ObjectId {
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    pub fn into_i64(self) -> i64 {
        self.0
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<ObjectId> for i64 {
    fn from(id: ObjectId) -> i64 {
        id.0
    }
}

impl From<i64> for ObjectId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataType {
    String,
    Bytes,
    Int64,
    Float64,
    Bool,
}

impl DataType {
    pub fn name(&self) -> &'static str {
        match self {
            DataType::String => "String",
            DataType::Bytes => "Bytes",
            DataType::Int64 => "Int64",
            DataType::Float64 => "Float64",
            DataType::Bool => "Bool",
        }
    }
}

pub trait DetectDataType {
    const TYPE: DataType;
}

macro_rules! impl_detect_data_type {
    ($t:ty, $dt:ident) => {
        impl DetectDataType for $t {
            const TYPE: DataType = DataType::$dt;
        }
    };
}

impl_detect_data_type!(String, String);
impl_detect_data_type!(Vec<u8>, Bytes);
impl_detect_data_type!(i64, Int64);
impl_detect_data_type!(f64, Float64);
impl_detect_data_type!(bool, Bool);

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq)]
pub enum Value<'a> {
    String(Cow<'a, str>),
    Bytes(Cow<'a, [u8]>),
    Int64(i64),
    Float64(f64),
    Bool(bool),
}

pub trait ValueConvert: Sized {
    fn to_value(&self) -> Value<'static>;

    fn from_value(value: &Value<'_>) -> Self;
}

impl ValueConvert for i64 {
    fn to_value(&self) -> Value<'static> {
        Value::Int64(*self)
    }

    fn from_value(value: &Value<'_>) -> Self {
        match value {
            Value::Int64(i) => *i,
            _ => panic!("Invalid value type"),
        }
    }
}

impl ValueConvert for f64 {
    fn to_value(&self) -> Value<'static> {
        Value::Float64(*self)
    }

    fn from_value(value: &Value<'_>) -> Self {
        match value {
            Value::Float64(f) => *f,
            _ => panic!("Invalid value type"),
        }
    }
}

impl ValueConvert for bool {
    fn to_value(&self) -> Value<'static> {
        Value::Bool(*self)
    }

    fn from_value(value: &Value<'_>) -> Self {
        match value {
            Value::Bool(b) => *b,
            _ => panic!("Invalid value type"),
        }
    }
}

impl ValueConvert for String {
    fn to_value(&self) -> Value<'static> {
        Value::String(Cow::Owned(self.clone()))
    }

    fn from_value(value: &Value<'_>) -> Self {
        match value {
            Value::String(s) => s.to_string(),
            _ => panic!("Invalid value type"),
        }
    }
}

impl ValueConvert for Vec<u8> {
    fn to_value(&self) -> Value<'static> {
        Value::Bytes(Cow::Owned(self.clone()))
    }

    fn from_value(value: &Value<'_>) -> Self {
        match value {
            Value::Bytes(b) => b.to_vec(),
            _ => panic!("Invalid value type"),
        }
    }
}

impl<'a> ToSql for Value<'a> {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Value::String(s) => Ok(ToSqlOutput::from(s.as_ref())),
            Value::Bytes(b) => Ok(ToSqlOutput::from(b.as_ref())),
            Value::Int64(i) => Ok(ToSqlOutput::from(*i)),
            Value::Float64(f) => Ok(ToSqlOutput::from(*f)),
            Value::Bool(b) => Ok(ToSqlOutput::from(*b)),
        }
    }
}

impl<'a> FromSql for Value<'a> {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        match value {
            rusqlite::types::ValueRef::Text(s) => {
                Ok(String::from_utf8(s.to_vec()).unwrap().to_value())
            }
            rusqlite::types::ValueRef::Blob(b) => Ok(b.to_vec().to_value()),
            rusqlite::types::ValueRef::Integer(i) => Ok(i.to_value()),
            rusqlite::types::ValueRef::Real(f) => Ok(f.to_value()),
            rusqlite::types::ValueRef::Null => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

impl<'a> Value<'a> {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::String(_) => DataType::String,
            Value::Bytes(_) => DataType::Bytes,
            Value::Int64(_) => DataType::Int64,
            Value::Float64(_) => DataType::Float64,
            Value::Bool(_) => DataType::Bool,
        }
    }

    pub fn sql_type(&self) -> &'static str {
        match self {
            Value::String(_) => "Text",
            Value::Bytes(_) => "Blob",
            Value::Int64(_) | Value::Bool(_) => "Integer",
            Value::Float64(_) => "Real",
        }
    }

    pub fn convert<T: ValueConvert>(&self) -> T {
        T::from_value(self)
    }
}
