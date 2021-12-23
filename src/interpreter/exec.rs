use crate::{
    interpreter::{dot_cmd, select_stmt},
    schema::DbSchema,
    syntax::{SqlStmt, Sqlite},
};
use anyhow::{bail, Result};

pub fn sqlite(sql: Sqlite, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
    match sql {
        Sqlite::DotCmd(cmd) => dot_cmd::run(cmd, db_schema),
        Sqlite::SqlStmt(stmt) => sql_stmt(stmt, db_schema, db),
    }
}

fn sql_stmt(stmt: SqlStmt, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
    match stmt {
        SqlStmt::Select(select_stmt) => select_stmt::run(&select_stmt, db_schema, db),
        _ => bail!("Not implemented: {:#?}", stmt),
    }
}
