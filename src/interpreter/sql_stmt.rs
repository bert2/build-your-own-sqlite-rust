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
    fn is_count_expr(cols: &[Expr]) -> bool {
        cols.len() == 1 && cols[0] == Expr::Count
    }

    match stmt {
        SqlStmt::Select(select_stmt) => {
            let page_size = db_schema.db_header.page_size.into();
            let tbl_schema = db_schema
                .table(select_stmt.tbl)
                .ok_or_else(|| anyhow!("Table '{}' not found", select_stmt.tbl))?;

            validate_col_names(&select_stmt, tbl_schema)?;

            if is_count_expr(&select_stmt.cols) {
                count_rows(tbl_schema, &select_stmt.filter, page_size, db)?;
            } else {
                select_cols(tbl_schema, &select_stmt, db_schema, page_size, db)?;
            }
        }
        _ => bail!("Not implemented: {:#?}", stmt),
    }

    Ok(())
}

fn count_rows(
    tbl_schema: &ObjSchema,
    filter: &Option<BoolExpr>,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let count = Page::parse(tbl_schema.rootpage, page_size, db)?
        .leaf_pages(page_size, db)
        .flat_map_ok_and_then(|page| {
            page.cell_ptrs()
                .map(move |cell_ptr| LeafTblCell::parse(&page.data[cell_ptr..]))
        })
        .filter_ok(|cell| match &filter {
            Some(expr) => match expr.eval(cell, tbl_schema).unwrap() {
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
    tbl_schema: &ObjSchema,
    select_stmt: &Select,
    db_schema: &DbSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let rootpage = Page::parse(tbl_schema.rootpage, page_size, db)?;

    if let Some(pk) = by_int_pk(select_stmt, tbl_schema) {
        int_pk_search(pk, &rootpage, select_stmt, tbl_schema, page_size, db)?;
    } else if let Some((idx, key)) = by_idx_key(select_stmt, db_schema) {
        idx_search(key, idx, &rootpage, select_stmt, tbl_schema, page_size, db)?;
    } else {
        tbl_search(rootpage, select_stmt, tbl_schema, page_size, db)?;
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
    rootpage: &Page,
    select_stmt: &Select,
    tbl: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    rootpage
        .find_cell(pk, page_size, db)?
        .iter()
        .for_each(|cell| println!("{}", print_row(cell, select_stmt, tbl)));

    Ok(())
}

fn idx_search(
    key: &Literal,
    idx: &ObjSchema,
    rootpage: &Page,
    select_stmt: &Select,
    tbl: &ObjSchema,
    page_size: usize,
    db: &[u8],
) -> Result<()> {
    let rows = Page::parse(idx.rootpage, page_size, db)?
        .find_idx_cells(key.into(), page_size, db)
        .map_ok_and_then(|cell| i64::try_from(&cell.payload[1]))
        .map_ok_and_then(|row_id| rootpage.find_cell(row_id, page_size, db))
        .flatten_ok()
        .map_ok(|cell| print_row(&cell, select_stmt, tbl));

    for row in rows {
        println!("{}", row?);
    }

    Ok(())
}

fn tbl_search(
    rootpage: Page,
    select_stmt: &Select,
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
        .filter_ok(move |cell| match &select_stmt.filter {
            Some(expr) => match expr.eval(cell, tbl).unwrap() {
                Value::Int(b) => b == 1,
                _ => panic!("BoolExpr didn't return a Value::Int"),
            },
            None => true,
        })
        .map_ok(move |cell| print_row(&cell, select_stmt, tbl));

    for row in rows {
        println!("{}", row?);
    }

    Ok(())
}

fn print_row(cell: &LeafTblCell, select_stmt: &Select, tbl_schema: &ObjSchema) -> String {
    select_stmt
        .selected_cols()
        .map(|col| {
            if tbl_schema.cols().is_int_pk(col) {
                cell.row_id.to_string()
            } else {
                format!("{}", cell.payload[tbl_schema.cols().record_pos(col)])
            }
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn validate_col_names(select_stmt: &Select, tbl_schema: &ObjSchema) -> Result<()> {
    let selected_cols = select_stmt.selected_cols();
    let filtered_cols = select_stmt
        .filter
        .iter()
        .flat_map(BoolExpr::referenced_cols);

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
