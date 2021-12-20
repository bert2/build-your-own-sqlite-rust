use crate::{schema::DbSchema, syntax::Sqlite};
use anyhow::Result;

pub fn sqlite(sql: Sqlite, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
    match sql {
        Sqlite::DotCmd(cmd) => dot_cmd::run(cmd, db_schema),
        Sqlite::SqlStmt(stmt) => sql_stmt::run(stmt, db_schema, db),
    }
}

mod dot_cmd {
    use crate::{schema::*, syntax::DotCmd};
    use anyhow::Result;
    use std::borrow::Cow;

    pub fn run(cmd: DotCmd, db_schema: &DbSchema) -> Result<()> {
        Ok(match cmd {
            DotCmd::DbInfo => dbinfo(db_schema),
            DotCmd::Tables => tables(db_schema),
            DotCmd::Schema => schema(db_schema),
        })
    }

    fn dbinfo(db_schema: &DbSchema) -> () {
        let s = db_schema;
        let h = &s.db_header;

        println!("database page size:  {}", h.page_size);
        println!("write format:        {}", h.write_format);
        println!("read format:         {}", h.read_format);
        println!("reserved bytes:      {}", h.reserved_bytes);
        println!("file change counter: {}", h.file_change_counter);
        println!("database page count: {}", h.db_page_count);
        println!("freelist page count: {}", h.freelist_page_count);
        println!("schema cookie:       {}", h.schema_cookie);
        println!("schema format:       {}", h.schema_format);
        println!("default cache size:  {}", h.default_cache_size);
        println!("autovacuum top root: {}", h.autovacuum_top_root);
        println!("incremental vacuum:  {}", h.incremental_vacuum);
        println!("text encoding:       {}", h.text_encoding);
        println!("user version:        {}", h.user_version);
        println!("application id:      {}", h.application_id);
        println!("software version:    {}", h.software_version);
        println!("number of tables:    {}", s.tables().count());
        println!("number of indexes:   {}", s.indexes().count());
        println!("number of triggers:  {}", s.triggers().count());
        println!("number of views:     {}", s.views().count());
        println!("schema size:         {}", s.size);
    }

    fn tables(db_schema: &DbSchema) -> () {
        db_schema
            .tables()
            .filter(|t| !t.is_sequence_tbl())
            .for_each(|t| print!("{} ", t.name));
    }

    fn schema(db_schema: &DbSchema) -> () {
        db_schema
            .objs
            .iter()
            .map(|schema| {
                schema.sql.map_or_else(
                    || format!("[Object '{}' has no CREATE statement]\n", schema.name).into(),
                    Cow::from,
                )
            })
            .for_each(|sql| print!("{} ", sql));
    }
}

mod sql_stmt {
    use crate::{
        format::Page,
        interpreter::eval::{Eval, Value},
        schema::*,
        syntax::*,
        util::*,
    };
    use anyhow::{anyhow, bail, Result};
    use itertools::Itertools;

    pub fn run(stmt: SqlStmt, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
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
                    count_rows(tbl, &filter, db_schema, db)?;
                } else {
                    select_cols(&get_col_names(&cols)?, tbl, &filter, db_schema, db)?;
                }
            }
            _ => bail!("Not implemented: {:#?}", stmt),
        }

        Ok(())
    }

    fn count_rows(
        tbl: &str,
        filter: &Option<BoolExpr>,
        db_schema: &DbSchema,
        db: &[u8],
    ) -> Result<()> {
        let page_size = db_schema.db_header.page_size.into();
        let schema = db_schema
            .table(tbl)
            .ok_or_else(|| anyhow!("Table '{}' not found", tbl))?;

        let count = Page::parse(schema.rootpage, page_size, db)?
            .leaf_pages(page_size, db)
            .flat_map_ok_and_then(Page::cells)
            .filter_ok(|cell| match &filter {
                Some(expr) => match expr.eval(cell, schema).unwrap() {
                    Value::Int(b) => b == 1,
                    _ => panic!("BoolExpr didn't return a Value::Int"),
                },
                None => true,
            })
            .fold_ok(0, |count, _| count + 1)?;

        println!("{}", count);
        Ok(())
    }

    fn select_cols(
        result_cols: &[&str],
        tbl: &str,
        filter: &Option<BoolExpr>,
        db_schema: &DbSchema,
        db: &[u8],
    ) -> Result<()> {
        let page_size = db_schema.db_header.page_size.into();
        let schema = db_schema
            .table(tbl)
            .ok_or_else(|| anyhow!("Table '{}' not found", tbl))?;

        filter
            .iter()
            .flat_map(BoolExpr::referenced_cols)
            .chain(result_cols.iter().copied())
            .try_for_each(|col| {
                if schema.cols().has(col) {
                    return Ok(());
                }

                bail!(
                    "Unknown column '{}'. Did you mean '{}'?",
                    col,
                    str_sim::most_similar(col, schema.cols().names()).unwrap()
                )
            })?;

        let indexed_col = filter.as_ref().and_then(BoolExpr::index_searchable_col);
        let _use_pk = indexed_col.map(|c| schema.cols().is_int_pk(c));

        //let _idx_schema = indexed_col.and_then(|c| db_schema.index(tbl, c));

        let rootpage = Page::parse(schema.rootpage, page_size, db)?;

        let rows = rootpage
            .leaf_pages(page_size, db)
            .flat_map_ok_and_then(Page::cells)
            .filter_ok(move |cell| match &filter {
                Some(expr) => match expr.eval(cell, schema).unwrap() {
                    Value::Int(b) => b == 1,
                    _ => panic!("BoolExpr didn't return a Value::Int"),
                },
                None => true,
            })
            .map_ok(move |cell| {
                result_cols
                    .iter()
                    .map(|res_col| {
                        if schema.cols().is_int_pk(res_col) {
                            cell.row_id.to_string()
                        } else {
                            format!("{}", cell.payload[schema.cols().record_pos(res_col)])
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            });

        for row in rows {
            println!("{}", row?)
        }

        Ok(())
    }
}
