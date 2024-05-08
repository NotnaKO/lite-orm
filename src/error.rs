#![forbid(unsafe_code)]

use thiserror::Error;

use crate::{data::DataType, ObjectId};

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    NotFound(Box<NotFoundError>),
    #[error(transparent)]
    UnexpectedType(Box<UnexpectedTypeError>),
    #[error(transparent)]
    MissingColumn(Box<MissingColumnError>),
    #[error("database is locked")]
    LockConflict,
    #[error("storage error: {0}")]
    Storage(#[source] Box<dyn std::error::Error>),
}

#[derive(Default)]
pub(crate) struct ErrorCtx {
    pub object_id: Option<ObjectId>,
    pub type_name: Option<&'static str>,
    pub attr_name: Option<&'static str>,
    pub table_name: Option<&'static str>,
    pub column_name: Option<&'static str>,
    pub expected_type: Option<DataType>,
    pub got_type: Option<String>,
}

impl ErrorCtx {
    pub fn not_found(object_id: ObjectId, type_name: &'static str) -> Self {
        Self {
            object_id: Some(object_id),
            type_name: Some(type_name),
            ..Default::default()
        }
    }
}

pub(crate) struct ErrorWithCtx<'a, E> {
    pub error: E,
    pub ctx: ErrorCtx,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, E> ErrorWithCtx<'a, E> {
    pub fn new(error: E, ctx: ErrorCtx) -> Self {
        Self {
            error,
            ctx,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a> From<ErrorWithCtx<'a, rusqlite::Error>> for Error {
    fn from(err: ErrorWithCtx<rusqlite::Error>) -> Self {
        let ctx = err.ctx;
        match err.error {
            rusqlite::Error::QueryReturnedNoRows => Error::NotFound(Box::new(NotFoundError {
                object_id: ctx.object_id.unwrap(),
                type_name: ctx.type_name.unwrap(),
            })),
            rusqlite::Error::InvalidColumnType(..) => {
                Error::UnexpectedType(Box::new(UnexpectedTypeError {
                    type_name: ctx.type_name.unwrap(),
                    attr_name: ctx.attr_name.unwrap(),
                    table_name: ctx.table_name.unwrap(),
                    column_name: ctx.column_name.unwrap(),
                    expected_type: ctx.expected_type.unwrap(),
                    got_type: ctx.got_type.unwrap(),
                }))
            }
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ErrorCode::DatabaseBusy,
                    ..
                },
                _,
            ) => Error::LockConflict,

            rusqlite::Error::SqliteFailure(_, text)
                if text.as_ref().is_some_and(|text| {
                    text.contains("no such column:") || text.contains("has no column named")
                }) =>
            {
                Error::MissingColumn(Box::new(MissingColumnError {
                    type_name: ctx.type_name.unwrap(),
                    attr_name: ctx.attr_name.unwrap(),
                    table_name: ctx.table_name.unwrap(),
                    column_name: ctx.column_name.unwrap(),
                }))
            }

            _ => unimplemented!("error: {:?}", err.error),
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Self::from(ErrorWithCtx::new(err, ErrorCtx::default()))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("object is not found: type '{type_name}', id {object_id}")]
pub struct NotFoundError {
    pub object_id: ObjectId,
    pub type_name: &'static str,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "invalid type for {type_name}::{attr_name}: expected equivalent of {expected_type:?}, \
    got {got_type} (table: {table_name}, column: {column_name})"
)]
pub struct UnexpectedTypeError {
    pub type_name: &'static str,
    pub attr_name: &'static str,
    pub table_name: &'static str,
    pub column_name: &'static str,
    pub expected_type: DataType,
    pub got_type: String,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "missing a column for {type_name}::{attr_name} \
    (table: {table_name}, column: {column_name})"
)]
pub struct MissingColumnError {
    pub type_name: &'static str,
    pub attr_name: &'static str,
    pub table_name: &'static str,
    pub column_name: &'static str,
}

////////////////////////////////////////////////////////////////////////////////

pub type Result<T> = std::result::Result<T, Error>;
