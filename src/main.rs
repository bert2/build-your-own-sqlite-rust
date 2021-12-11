use anyhow::{anyhow, bail, Result};
use sqlite_starter_rust::{cell::*, page_header::*, schema::*, sql::*, str_sim::*, util::*};
use std::{borrow::*, collections::HashMap, convert::*, env::args, fs::File, io::prelude::*};

fn main() -> Result<()> {
    let args = validate(args().collect::<Vec<_>>())?;
    let db = read_db(&args[1])?;
    let output = parse_and_run(&args[2], &db)?;
    println!("{}", output);
    Ok(())
}

fn validate(args: Vec<String>) -> Result<Vec<String>> {
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => Ok(args),
    }
}

fn read_db(file: &str) -> Result<Vec<u8>> {
    let mut file = File::open(file)?;
    let mut db = Vec::new();
    file.read_to_end(&mut db)?;
    Ok(db)
}

fn parse_and_run(sql: &str, db: &Vec<u8>) -> Result<String> {
    match parse_sqlite(sql) {
        Ok(Sqlite::DotCmd(cmd)) => run_dot_cmd(cmd, db),
        Ok(Sqlite::SqlStmt(stmt)) => run_sql_stmt(stmt, db),
        Err(e) => bail!("Invalid SQL: {}", e),
    }
}

fn run_dot_cmd(cmd: DotCmd, db: &Vec<u8>) -> Result<String> {
    match cmd {
        DotCmd::DbInfo => dbinfo(db),
        DotCmd::Tables => tables(db),
        DotCmd::Schema => schema(db),
    }
}

fn run_sql_stmt(stmt: SqlStmt, db: &Vec<u8>) -> Result<String> {
    fn is_count_expr(cols: &[Expr]) -> bool {
        cols.len() == 1 && cols[0] == Expr::Count
    }

    fn get_col_names(cols: Vec<Expr>) -> Result<Vec<&str>> {
        cols.iter()
            .map(|c| match c {
                Expr::ColName(name) => Ok(*name),
                _ => bail!("Unexpected expression among result columns: {:?}", c),
            })
            .collect::<Result<Vec<_>>>()
    }

    match stmt {
        SqlStmt::Select { cols, tbl } => {
            if is_count_expr(&cols) {
                count_rows(tbl, db)
            } else {
                select_cols(get_col_names(cols)?, tbl, db)
            }
        }
        _ => bail!("Not implemented: {:#?}", stmt),
    }
}

fn dbinfo(db: &Vec<u8>) -> Result<String> {
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

fn tables(db: &Vec<u8>) -> Result<String> {
    Ok(DbSchema::parse(db)?
        .tables()
        .map(|t| t.name)
        .collect::<Vec<_>>()
        .join(" "))
}

fn schema(db: &Vec<u8>) -> Result<String> {
    fn get_sql<'a>(schema: &Schema<'a>) -> Cow<'a, str> {
        schema
            .sql
            .map(Cow::from)
            .unwrap_or_else(|| format!("[Object '{}' has no CREATE statement]", schema.name).into())
    }

    Ok(DbSchema::parse(db)?
        .tables()
        .map(get_sql)
        .collect::<Vec<_>>()
        .join("\n"))
}

fn count_rows(tbl: &str, db: &Vec<u8>) -> Result<String> {
    let db_schema = DbSchema::parse(db)?;
    let page_size: usize = db_schema.db_header.page_size.into();
    let tbl_schema = db_schema
        .table(tbl)
        .ok_or(anyhow!("Table '{}' not found", tbl))?;
    let page_offset = tbl_schema.offset(page_size);
    let page_header = PageHeader::parse(&db[page_offset..])?;
    Ok(format!("{}", page_header.number_of_cells))
}

fn select_cols(result_cols: Vec<&str>, tbl: &str, db: &Vec<u8>) -> Result<String> {
    let db_schema = DbSchema::parse(db)?;
    let page_size: usize = db_schema.db_header.page_size.into();
    let tbl_schema = db_schema
        .table(tbl)
        .ok_or(anyhow!("Table '{}' not found", tbl))?;
    let tbl_sql = tbl_schema
        .sql
        .ok_or_else(|| anyhow!("No CREATE statment for object '{}' found", tbl_schema.name))?;

    let tbl_col_defs = match parse_sql_stmt(tbl_sql)? {
        SqlStmt::CreateTbl { col_defs, .. } => col_defs,
        _ => bail!("Expected CREATE TABLE statement but got:\n{}", tbl_sql),
    };
    let pk_col_name = tbl_col_defs
        .iter()
        .find(ColDef::is_int_pk)
        .map(ColDef::name);
    let tbl_col_names = tbl_col_defs.iter().map(ColDef::name).collect::<Vec<_>>();

    result_cols.iter().try_for_each(|col| {
        if !tbl_col_names.contains(col) {
            bail!(
                "Unknown column '{}'. Did you mean '{}'?",
                col,
                most_similar(col, &tbl_col_names).unwrap()
            )
        } else {
            Ok(())
        }
    })?;

    let col_name_to_idx = tbl_col_names
        .iter()
        .enumerate()
        .map(flip)
        .collect::<HashMap<_, _>>();

    let page_offset = tbl_schema.offset(page_size);

    let page = &db[page_offset..page_offset + page_size];

    let page_header = PageHeader::parse(&page)?;

    let col_values = page[page_header.size()..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| usize::from(u16::from_be_bytes(bytes.try_into().unwrap())))
        .map(|cell_pointer| {
            Cell::parse(&page[cell_pointer..]).map(|cell| {
                result_cols
                    .iter()
                    .map(|res_col| {
                        if opt_contains(&pk_col_name, res_col) {
                            cell.row_id.to_string()
                        } else {
                            format!("{}", &cell.payload[col_name_to_idx[res_col]])
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect::<Result<Vec<_>>>()?
        .join("\n");

    Ok(col_values)
}
