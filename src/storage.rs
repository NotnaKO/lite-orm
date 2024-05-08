#![forbid(unsafe_code)]

use std::fmt::Write;

use rusqlite::ToSql;

use crate::{
    data::{DataType, Value},
    error::{Error, ErrorCtx, ErrorWithCtx, Result, UnexpectedTypeError},
    object::Schema,
    ObjectId,
};

////////////////////////////////////////////////////////////////////////////////

pub type Row<'a> = Vec<Value<'a>>;
pub type RowSlice<'a> = [Value<'a>];

////////////////////////////////////////////////////////////////////////////////

pub(crate) trait StorageTransaction {
    fn table_exists(&self, table: &str) -> Result<bool>;
    fn create_table(&self, schema: &Schema) -> Result<()>;

    fn insert_row(&self, schema: &Schema, row: &RowSlice) -> Result<ObjectId>;
    fn update_row(&self, id: ObjectId, schema: &Schema, row: &RowSlice) -> Result<()>;
    fn select_row(&self, id: ObjectId, schema: &Schema) -> Result<Row<'static>>;
    fn delete_row(&self, id: ObjectId, schema: &Schema) -> Result<()>;

    fn commit(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
}

macro_rules! write_columns {
    ($sql:ident, $schema:ident) => {
        let tmp = $schema
            .columns
            .iter()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>();
        write!($sql, "{}", tmp.join(", ")).unwrap();
    };
}

fn error_by_scheme(schema: &Schema, e: rusqlite::Error, id: ObjectId) -> Error {
    Error::from(match &e {
        rusqlite::Error::QueryReturnedNoRows => {
            ErrorWithCtx::new(e, ErrorCtx::not_found(id, schema.type_name))
        }
        rusqlite::Error::InvalidColumnType(i, t, ty) => ErrorWithCtx::new(
            rusqlite::Error::InvalidColumnType(*i, t.clone(), ty.clone()),
            ErrorCtx {
                table_name: schema.table_name.into(),
                type_name: schema.type_name.into(),
                attr_name: schema.attrs[*i].into(),
                column_name: schema.columns[*i].0.into(),
                expected_type: schema.columns[*i].1.into(),
                got_type: ty.to_string().into(),
                ..Default::default()
            },
        ),
        rusqlite::Error::SqliteFailure(_, text)
            if text.as_ref().is_some_and(|text| {
                text.contains("no such column:") || text.contains("has no column named")
            }) =>
        {
            let mut ctx = ErrorCtx {
                table_name: schema.table_name.into(),
                type_name: schema.type_name.into(),
                ..Default::default()
            };

            let text = text.as_ref().unwrap();
            let column_name = if text.contains("no such column:") {
                text.split("no such column:").last().unwrap().trim()
            } else {
                text.split("has no column named").last().unwrap().trim()
            };

            let pos = schema
                .columns
                .iter()
                .position(|(name, _)| *name == column_name)
                .unwrap();

            ctx.column_name = schema.columns[pos].0.into();
            ctx.attr_name = schema.attrs[pos].into();

            ErrorWithCtx::new(e, ctx)
        }
        _ => ErrorWithCtx::new(e, ErrorCtx::default()),
    })
}

fn row_exists(tx: &rusqlite::Transaction, id: ObjectId, schema: &Schema) -> Result<()> {
    let sql = format!("SELECT 1 FROM {} WHERE id = ?", schema.table_name);
    tx.query_row(&sql, [&id], |_| Ok(()))
        .map_err(|e| error_by_scheme(schema, e, id))
}

impl<'a> StorageTransaction for rusqlite::Transaction<'a> {
    fn table_exists(&self, table: &str) -> Result<bool> {
        let exists = self
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
                [&table],
                |_| Ok(()),
            )
            .is_ok();
        Ok(exists)
    }

    fn create_table(&self, schema: &Schema) -> Result<()> {
        let mut sql = format!("CREATE TABLE {} (", schema.table_name);
        let mut columns = vec!["id INTEGER PRIMARY KEY AUTOINCREMENT".to_string()];
        for (name, ty) in schema.columns {
            columns.push(format!("{} {}", name, ty));
        }
        write!(&mut sql, "{}", columns.join(", ")).unwrap();
        write!(&mut sql, ")").unwrap();
        self.execute(&sql, []).map_err(Error::from)?;
        Ok(())
    }

    fn insert_row(&self, schema: &Schema, row: &RowSlice) -> Result<ObjectId> {
        let mut sql = format!("INSERT INTO {}", schema.table_name);
        if !row.is_empty() {
            write!(&mut sql, " (").unwrap();
            write_columns!(sql, schema);
            write!(&mut sql, ") VALUES (").unwrap();
            write!(&mut sql, "{}", vec!["?"; row.len()].join(", ")).unwrap();
            write!(&mut sql, ")").unwrap();
        } else {
            write!(&mut sql, " DEFAULT VALUES").unwrap();
        }
        let params: Vec<&dyn ToSql> = row.iter().map(|x| x as &dyn ToSql).collect();
        self.execute(&sql, params.as_slice())
            .map_err(|e| error_by_scheme(schema, e, ObjectId::new(self.last_insert_rowid())))?;
        Ok(ObjectId::new(self.last_insert_rowid()))
    }

    fn update_row(&self, id: ObjectId, schema: &Schema, row: &RowSlice) -> Result<()> {
        if schema.columns.is_empty() {
            return row_exists(self, id, schema);
        }
        let mut sql = format!("UPDATE {} SET ", schema.table_name);
        let mut columns = Vec::new();
        for (name, _) in schema.columns.iter() {
            columns.push(format!("{} = ?", name));
        }
        write!(&mut sql, "{}", columns.join(", ")).unwrap();
        write!(&mut sql, " WHERE id = ?").unwrap();

        let mut params: Vec<&dyn ToSql> = row.iter().map(|x| x as &dyn ToSql).collect();
        params.push(&id);
        self.execute(&sql, params.as_slice())
            .map_err(|e| error_by_scheme(schema, e, id))?;
        Ok(())
    }

    fn select_row(&self, id: ObjectId, schema: &Schema) -> Result<Row<'static>> {
        if schema.columns.is_empty() {
            return row_exists(self, id, schema).map(|_| Vec::new());
        }
        let mut sql = "SELECT ".to_string();
        write_columns!(sql, schema);
        write!(&mut sql, " FROM {} WHERE id = ?", schema.table_name).unwrap();

        let val = self.query_row(&sql, [&id], |row| {
            let mut result = Vec::new();
            for i in 0.. {
                match row.get(i) {
                    Ok(val) => result.push(val),
                    Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                    Err(e) => return Err(e),
                };
            }
            Ok(result)
        });
        let val = val.map_err(|e| error_by_scheme(schema, e, id))?;
        convert_by_schema(val, schema)
    }

    fn delete_row(&self, id: ObjectId, schema: &Schema) -> Result<()> {
        let sql = format!("DELETE FROM {} WHERE id = ?", schema.table_name);
        self.execute(&sql, [&id]).map_err(Error::from)?;
        Ok(())
    }

    fn commit(&self) -> Result<()> {
        let sql = "COMMIT".to_string();
        self.execute(&sql, []).map_err(Error::from)?;
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        let sql = "ROLLBACK".to_string();
        self.execute(&sql, []).map_err(Error::from)?;
        Ok(())
    }
}

fn convert_by_schema<'a>(mut val: Row<'a>, schema: &Schema) -> Result<Row<'a>> {
    debug_assert_eq!(val.len(), schema.columns.len());

    let mut result = Vec::with_capacity(val.len());
    for ((i, (column_name, ty)), v) in schema.columns.iter().enumerate().zip(val.iter_mut()) {
        match (ty, v) {
            (ty, v) if v.data_type() == *ty => result.push(v.clone()),
            (DataType::Bool, Value::Int64(i)) if matches!(*i, 0..=1) => {
                result.push(Value::Bool(*i != 0));
            }
            (_, v) => {
                return Err(Error::UnexpectedType(Box::new(UnexpectedTypeError {
                    type_name: schema.type_name,
                    attr_name: schema.attrs[i],
                    table_name: schema.table_name,
                    column_name,
                    expected_type: *ty,
                    got_type: v.sql_type().to_string(),
                })));
            }
        }
    }

    Ok(result)
}
