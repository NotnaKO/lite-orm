#![forbid(unsafe_code)]
use crate::{data::DataType, storage::Row};
use std::any::Any;
use std::hash::{Hash, Hasher};

////////////////////////////////////////////////////////////////////////////////

pub trait Object: Any + Sized {
    fn schema() -> &'static Schema;

    fn from_row(row: Row<'_>) -> Self;
    fn to_row(&self) -> Row<'_>;
}

pub trait Store: Any {
    fn as_any(&self) -> &dyn Any;

    fn as_mut_any(&mut self) -> &mut dyn Any;

    fn to_row(&self) -> Row<'_>;
}

impl<T: Object> Store for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }

    fn to_row(&self) -> Row<'_> {
        self.to_row()
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schema {
    pub table_name: &'static str,
    pub type_name: &'static str,
    pub columns: &'static [(&'static str, DataType)],
    pub attrs: &'static [&'static str],
}

impl Hash for Schema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table_name.hash(state);
    }
}
