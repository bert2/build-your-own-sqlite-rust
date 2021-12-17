use crate::syntax::ast::Sqlite;
use anyhow::Result;

pub fn sqlite(sql: Sqlite, db: &[u8]) -> Result<String> {
    match sql {
        Sqlite::DotCmd(cmd) => dot_cmd::run(cmd, db),
        Sqlite::SqlStmt(stmt) => sql_stmt::run(stmt, db),
    }
}

mod dot_cmd {
    use crate::{schema::*, syntax::ast::DotCmd};
    use anyhow::Result;
    use std::{borrow::Cow, convert::Into};

    pub fn run(cmd: DotCmd, db: &[u8]) -> Result<String> {
        match cmd {
            DotCmd::DbInfo => dbinfo(db),
            DotCmd::Tables => tables(db),
            DotCmd::Schema => schema(db),
        }
    }

    fn dbinfo(db: &[u8]) -> Result<String> {
        let s = DbSchema::parse(db)?;
        let h = &s.db_header;
        let mut o = vec![];

        o.push(format!("database page size:  {}", h.page_size));
        o.push(format!("write format:        {}", h.write_format));
        o.push(format!("read format:         {}", h.read_format));
        o.push(format!("reserved bytes:      {}", h.reserved_bytes));
        o.push(format!("file change counter: {}", h.file_change_counter));
        o.push(format!("database page count: {}", h.db_page_count));
        o.push(format!("freelist page count: {}", h.freelist_page_count));
        o.push(format!("schema cookie:       {}", h.schema_cookie));
        o.push(format!("schema format:       {}", h.schema_format));
        o.push(format!("default cache size:  {}", h.default_cache_size));
        o.push(format!("autovacuum top root: {}", h.autovacuum_top_root));
        o.push(format!("incremental vacuum:  {}", h.incremental_vacuum));
        o.push(format!("text encoding:       {}", h.text_encoding));
        o.push(format!("user version:        {}", h.user_version));
        o.push(format!("application id:      {}", h.application_id));
        o.push(format!("software version:    {}", h.software_version));
        o.push(format!("number of tables:    {}", s.tables().count()));
        o.push(format!("number of indexes:   {}", s.indexes().count()));
        o.push(format!("number of triggers:  {}", s.triggers().count()));
        o.push(format!("number of views:     {}", s.views().count()));
        o.push(format!("schema size:         {}", s.size));

        Ok(o.join("\n"))
    }

    fn tables(db: &[u8]) -> Result<String> {
        Ok(DbSchema::parse(db)?
            .tables()
            .map(|t| t.name)
            .collect::<Vec<_>>()
            .join(" "))
    }

    fn schema(db: &[u8]) -> Result<String> {
        fn get_sql<'a>(schema: &Schema<'a>) -> Cow<'a, str> {
            schema.sql.map(Cow::from).unwrap_or_else(|| {
                format!("[Object '{}' has no CREATE statement]", schema.name).into()
            })
        }

        Ok(DbSchema::parse(db)?
            .tables()
            .map(get_sql)
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

mod sql_stmt {
    use crate::{
        format::page::Page,
        interpreter::eval::{Eval, Value},
        schema::*,
        syntax::ast::*,
        util::str_sim,
    };
    use anyhow::{anyhow, bail, Result};
    use std::convert::Into;

    pub fn run(stmt: SqlStmt, db: &[u8]) -> Result<String> {
        fn is_count_expr(cols: &[ResultExpr]) -> bool {
            cols.len() == 1 && cols[0] == ResultExpr::Count
        }

        fn get_col_names<'a>(cols: &'a [ResultExpr]) -> Result<Vec<&'a str>> {
            cols.iter()
                .map(|c| match c {
                    ResultExpr::Value(Expr::ColName(name)) => Ok(*name),
                    _ => bail!("Unexpected expression among result columns: {:?}", c),
                })
                .collect::<Result<Vec<_>>>()
        }

        match stmt {
            SqlStmt::Select { cols, tbl, filter } => {
                if is_count_expr(&cols) {
                    count_rows(tbl, db)
                } else {
                    select_cols(&get_col_names(&cols)?, tbl, filter, db)
                }
            }
            _ => bail!("Not implemented: {:#?}", stmt),
        }
    }

    fn count_rows(tbl: &str, db: &[u8]) -> Result<String> {
        let (_, page) = load_tbl(tbl, db)?;
        Ok(format!("{}", page.header.number_of_cells))
    }

    fn select_cols(
        result_cols: &[&str],
        tbl: &str,
        filter: Option<BoolExpr>,
        db: &[u8],
    ) -> Result<String> {
        let (schema, page) = load_tbl(tbl, db)?;

        result_cols.iter().try_for_each(|col| {
            if !schema.cols().has(col) {
                bail!(
                    "Unknown column '{}'. Did you mean '{}'?",
                    col,
                    str_sim::most_similar(col, &schema.cols().names()).unwrap()
                )
            } else {
                Ok(())
            }
        })?;

        Ok(page
            .cells()?
            .iter()
            .filter(|cell| match &filter {
                Some(expr) => {
                    if let Value::Int(b) = expr.eval(cell, &schema) {
                        b == 1
                    } else {
                        panic!("Boolean expression didn't return an int value")
                    }
                }
                None => true,
            })
            .map(|cell| {
                result_cols
                    .iter()
                    .map(|res_col| {
                        if schema.cols().is_int_pk(res_col) {
                            cell.row_id.to_string()
                        } else {
                            format!("{}", cell.payload[schema.cols().index(res_col)])
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
            .collect::<Vec<_>>()
            .join("\n"))
    }

    fn load_tbl<'a>(tbl: &str, db: &'a [u8]) -> Result<(Schema<'a>, Page<'a>)> {
        let db_schema = DbSchema::parse(db)?;
        let page_size = db_schema.db_header.page_size.into();
        let schema = db_schema
            .table(tbl)
            .ok_or(anyhow!("Table '{}' not found", tbl))?;
        let page = Page::parse(schema.rootpage, page_size, db)?;

        Ok((schema, page))
    }
}