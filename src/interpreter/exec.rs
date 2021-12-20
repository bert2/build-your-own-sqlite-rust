use crate::{
    interpreter::{dot_cmd, sql_stmt},
    schema::DbSchema,
    syntax::Sqlite,
};
use anyhow::Result;

pub fn sqlite(sql: Sqlite, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
    match sql {
        Sqlite::DotCmd(cmd) => dot_cmd::run(cmd, db_schema),
        Sqlite::SqlStmt(stmt) => sql_stmt::run(stmt, db_schema, db),
    }
}
