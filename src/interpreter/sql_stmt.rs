use crate::{
    format::{LeafTblCell, Page},
    interpreter::eval::{Eval, Value},
    schema::*,
    syntax::*,
    util::{JoinOkExt, *},
};
use anyhow::{anyhow, bail, Result};
use itertools::Itertools;
use std::convert::{TryFrom, TryInto};

pub fn run(stmt: SqlStmt, db_schema: &DbSchema, db: &[u8]) -> Result<()> {
    match stmt {
        SqlStmt::Select(select_stmt) => {
            let page_size = db_schema.db_header.page_size.into();
            let tbl_schema = db_schema
                .table(select_stmt.tbl)
                .ok_or_else(|| anyhow!("Table '{}' not found", select_stmt.tbl))?;

            validate_col_names(&select_stmt, tbl_schema)?;

            select_cols(tbl_schema, &select_stmt, db_schema, page_size, db)?;
        }
        _ => bail!("Not implemented: {:#?}", stmt),
    }

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
    if let Some(cell) = rootpage.find_cell(pk, page_size, db)? {
        let mut row = eval_row(cell, select_stmt, tbl);

        if select_stmt.has_count_expr() {
            println!("{}", replace_count(row, 1)?.join_ok("|")?);
        } else {
            println!("{}", row.join_ok("|")?);
        }
    }

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
    let mut rows = Page::parse(idx.rootpage, page_size, db)?
        .find_idx_cells(key.into(), page_size, db)
        .map_ok_and_then(|cell| i64::try_from(&cell.payload[1]))
        .map_ok_and_then(|row_id| rootpage.find_cell(row_id, page_size, db))
        .flatten_ok()
        .map_ok(|cell| eval_row(cell, select_stmt, tbl));

    if select_stmt.has_count_expr() {
        if let Some(first) = rows.next().transpose()? {
            println!("{}", replace_count(first, rows.count() + 1)?.join_ok("|")?);
        }
    } else {
        for row in rows {
            println!("{}", row?.join_ok("|")?);
        }
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
    let mut rows = rootpage
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
        .map_ok(|cell| eval_row(cell, select_stmt, tbl));

    if select_stmt.has_count_expr() {
        if let Some(first) = rows.next().transpose()? {
            println!("{}", replace_count(first, rows.count() + 1)?.join_ok("|")?);
        }
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
    row: impl Iterator<Item = Result<Value<'a>>> + 'a,
    count: usize,
) -> Result<impl Iterator<Item = Result<Value<'a>>> + 'a>
where
{
    let count = count.try_into()?;
    Ok(row.map_ok(move |col| match col {
        Value::CountPlaceholder => Value::Int(count),
        _ => col,
    }))
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
