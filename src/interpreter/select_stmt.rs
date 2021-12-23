use crate::{
    format::{LeafTblCell, Page},
    interpreter::{
        btree,
        eval::{Eval, Value},
    },
    schema::{DbSchema, ObjSchema},
    syntax::{BoolExpr, Expr, Literal, Select},
    util::{str_sim, IterEither, JoinOkExt},
};
use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use std::convert::TryInto;

pub fn run(select_stmt: &Select, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
    let page_size = db_schema.db_header.page_size.into();
    let tbl_schema = db_schema
        .table(select_stmt.tbl)
        .ok_or_else(|| anyhow!("Table '{}' not found", select_stmt.tbl))?;
    let rootpage = Page::parse(tbl_schema.rootpage, page_size, db)?;

    validate_col_names(select_stmt, tbl_schema)?;

    if let Some(pk) = by_int_pk(select_stmt, tbl_schema) {
        int_pk_search(pk, select_stmt, &rootpage, tbl_schema, page_size, db)?;
    } else if let Some((idx_schema, key)) = by_idx_key(select_stmt, db_schema) {
        idx_search(
            key,
            idx_schema,
            select_stmt,
            &rootpage,
            tbl_schema,
            page_size,
            db,
        )?;
    } else {
        full_tbl_search(select_stmt, rootpage, tbl_schema, page_size, db)?;
    }

    Ok(())
}

fn by_int_pk(select_stmt: &Select, schema: &ObjSchema) -> Option<i64> {
    select_stmt
        .filter
        .as_ref()
        .and_then(BoolExpr::is_int_pk_servable)
        .filter(|(col, _)| schema.cols().is_int_pk(col))
        .map(|(_, pk)| pk)
}

fn by_idx_key<'a>(
    select_stmt: &'a Select,
    db_schema: &'a DbSchema,
) -> Option<(&'a ObjSchema<'a>, &'a Literal<'a>)> {
    select_stmt
        .filter
        .as_ref()
        .and_then(BoolExpr::is_index_servable)
        .and_then(|(col, key)| db_schema.index(select_stmt.tbl, col).map(|idx| (idx, key)))
}

fn int_pk_search(
    pk: i64,
    select_stmt: &Select,
    tbl_page: &Page,
    tbl_schema: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let row = btree::pk_scan(pk, tbl_page, page_size, db)?
        .map(|cell| eval_row(cell, select_stmt, tbl_schema))
        .ok_or(select_stmt);

    if select_stmt.has_count_expr() {
        println!("{}", replace_count(row, 1)?.join_ok("|")?);
    } else if let Ok(mut row) = row {
        println!("{}", row.join_ok("|")?);
    }

    Ok(())
}

fn idx_search(
    key: &Literal,
    idx_schema: &ObjSchema,
    select_stmt: &Select,
    tbl_page: &Page,
    tbl_schema: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let idx_page = Page::parse(idx_schema.rootpage, page_size, db)?;
    let mut rows = btree::idx_scan(key.into(), idx_page, tbl_page, page_size, db)
        .map_ok(|cell| eval_row(cell, select_stmt, tbl_schema));

    if select_stmt.has_count_expr() {
        let first = rows.next().transpose()?.ok_or(select_stmt);
        println!("{}", replace_count(first, rows.count() + 1)?.join_ok("|")?);
    } else {
        for row in rows {
            println!("{}", row?.join_ok("|")?);
        }
    }

    Ok(())
}

fn full_tbl_search(
    select_stmt: &Select,
    tbl_page: Page,
    tbl_schema: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let mut rows = btree::full_tbl_scan(tbl_page, page_size, db)
        .filter_ok(move |cell| match &select_stmt.filter {
            Some(expr) => match expr.eval(cell, tbl_schema).unwrap() {
                Value::Int(b) => b == 1,
                _ => panic!("BoolExpr didn't return a Value::Int"),
            },
            None => true,
        })
        .map_ok(|cell| eval_row(cell, select_stmt, tbl_schema));

    if select_stmt.has_count_expr() {
        let first = rows.next().transpose()?.ok_or(select_stmt);
        println!("{}", replace_count(first, rows.count() + 1)?.join_ok("|")?);
    } else {
        for row in rows {
            println!("{}", row?.join_ok("|")?);
        }
    }

    Ok(())
}

fn eval_row<'a>(
    cell: LeafTblCell<'a>,
    select_stmt: &'a Select,
    tbl_schema: &'a ObjSchema,
) -> impl Iterator<Item = Result<Value<'a>>> {
    select_stmt
        .cols
        .iter()
        .map(move |col| col.eval(&cell, tbl_schema))
}

fn replace_count<'a>(
    row: Result<impl Iterator<Item = Result<Value<'a>>> + 'a, &'a Select>,
    count: usize,
) -> Result<impl Iterator<Item = Result<Value<'a>>> + 'a>
where
{
    match row {
        Ok(row) => {
            let count = count.try_into()?;
            let row = row.map_ok(move |col| match col {
                Value::CountPlaceholder => Value::Int(count),
                _ => col,
            });
            Ok(IterEither::left(row))
        }
        Err(select_stmt) => {
            let empty_row = select_stmt.cols.iter().map(|col| match col {
                Expr::Count => Ok(Value::Int(0)),
                Expr::Literal(lit) => Ok(lit.into()),
                Expr::ColName(_) => Ok(Value::String("")),
            });
            Ok(IterEither::right(empty_row))
        }
    }
}

fn validate_col_names(select_stmt: &Select, tbl_schema: &ObjSchema) -> Result<()> {
    let selected_cols = select_stmt.selected_col_names();
    let filtered_cols = select_stmt
        .filter
        .iter()
        .flat_map(BoolExpr::referenced_col_names);

    selected_cols.chain(filtered_cols).try_for_each(|col| {
        if tbl_schema.cols().has(col) {
            return Ok(());
        }

        bail!(
            "Unknown column '{}'. Did you mean '{}'?",
            col,
            str_sim::most_similar(col, tbl_schema.cols().names()).unwrap()
        )
    })
}
