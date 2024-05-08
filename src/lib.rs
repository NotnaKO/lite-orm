#![forbid(unsafe_code)]

pub use connection::Connection;
pub use data::ObjectId;
pub use data::ValueConvert;
pub use error::{Error, Result};
pub use object::Object;
pub use orm_derive::Object;
pub use transaction::{ObjectState, Transaction, Tx};

mod connection;
mod error;
mod transaction;

pub mod data;
pub mod object;
pub mod storage;
