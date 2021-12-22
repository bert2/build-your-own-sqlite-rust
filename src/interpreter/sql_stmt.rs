use crate::{
    format::{LeafTblCell, Page},
    interpreter::eval::{Eval, Value},
    schema::*,
    syntax::*,
    util::*,
};
use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use std::convert::TryFrom;

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

fn count_rows(tbl: &str, filter: &Option<BoolExpr>, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
    let page_size = db_schema.db_header.page_size.into();
    let schema = db_schema
        .table(tbl)
        .ok_or_else(|| anyhow!("Table '{}' not found", tbl))?;

    validate_cols(&[], filter, schema)?;

    let count = Page::parse(schema.rootpage, page_size, db)?
        .leaf_pages(page_size, db)
        .flat_map_ok_and_then(|page| {
            page.cell_ptrs()
                .map(move |cell_ptr| LeafTblCell::parse(&page.data[cell_ptr..]))
        })
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

    validate_cols(result_cols, filter, schema)?;

    let rootpage = Page::parse(schema.rootpage, page_size, db)?;

    if let Some(pk) = by_int_pk(filter, schema) {
        int_pk_search(pk, &rootpage, result_cols, schema, page_size, db)?;
    } else if let Some((idx, key)) = by_idx_key(filter, tbl, db_schema) {
        idx_search(key, idx, &rootpage, result_cols, schema, page_size, db)?;
    } else {
        tbl_search(filter, rootpage, result_cols, schema, page_size, db)?;
    }

    Ok(())
}

fn by_int_pk(filter: &Option<BoolExpr>, schema: &ObjSchema) -> Option<i64> {
    filter
        .as_ref()
        .and_then(BoolExpr::int_pk_servable)
        .filter(|(col, _)| schema.cols().is_int_pk(col))
        .map(|(_, pk)| pk)
}

fn by_idx_key<'a>(
    filter: &'a Option<BoolExpr>,
    tbl: &str,
    db_schema: &'a DbSchema,
) -> Option<(&'a ObjSchema<'a>, &'a Expr<'a>)> {
    filter
        .as_ref()
        .and_then(BoolExpr::index_servable)
        .and_then(|(col, key)| db_schema.index(tbl, col).map(|idx| (idx, key)))
}

fn int_pk_search(
    pk: i64,
    rootpage: &Page,
    result_cols: &[&str],
    tbl: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    rootpage
        .find_cell(pk, page_size, db)?
        .iter()
        .for_each(|cell| println!("{}", print_row(cell, result_cols, tbl)));

    Ok(())
}

fn idx_search(
    key: &Expr,
    idx: &ObjSchema,
    rootpage: &Page,
    result_cols: &[&str],
    tbl: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let rows = Page::parse(idx.rootpage, page_size, db)?
        .find_idx_cells(Value::try_from(key)?, page_size, db)
        .map_ok_and_then(|cell| i64::try_from(&cell.payload[1]))
        .map_ok_and_then(|row_id| rootpage.find_cell(row_id, page_size, db))
        .flatten_ok()
        .map_ok(|cell| print_row(&cell, result_cols, tbl));

    for row in rows {
        println!("{}", row?);
    }

    Ok(())
}

fn tbl_search(
    filter: &Option<BoolExpr>,
    rootpage: Page,
    result_cols: &[&str],
    tbl: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let rows = rootpage
        .leaf_pages(page_size, db)
        .flat_map_ok_and_then(|page| {
            page.cell_ptrs()
                .map(move |cell_ptr| LeafTblCell::parse(&page.data[cell_ptr..]))
        })
        .filter_ok(move |cell| match &filter {
            Some(expr) => match expr.eval(cell, tbl).unwrap() {
                Value::Int(b) => b == 1,
                _ => panic!("BoolExpr didn't return a Value::Int"),
            },
            None => true,
        })
        .map_ok(move |cell| print_row(&cell, result_cols, tbl));

    for row in rows {
        println!("{}", row?);
    }

    Ok(())
}

fn print_row(cell: &LeafTblCell, result_cols: &[&str], schema: &ObjSchema) -> String {
    result_cols
        .iter()
        .map(|col| {
            if schema.cols().is_int_pk(col) {
                cell.row_id.to_string()
            } else {
                format!("{}", cell.payload[schema.cols().record_pos(col)])
            }
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn validate_cols(
    result_cols: &[&str],
    filter: &Option<BoolExpr>,
    schema: &ObjSchema,
) -> Result<()> {
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
        })
}
