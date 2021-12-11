use anyhow::{anyhow, bail, Result};
use sqlite_starter_rust::{
    cell::*, db_header::*, page_header::*, schema::*, sql::*, str_sim::*, util::*,
};
use std::{borrow::*, collections::HashMap, convert::*, env::args, fs::File, io::prelude::*};

fn main() -> Result<()> {
    let args = validate(args().collect::<Vec<_>>())?;
    let db = read_db(&args[1])?;
    parse_and_run(&args[2], &db)
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

fn parse_and_run(sql: &str, db: &Vec<u8>) -> Result<()> {
    match parse_sqlite(sql) {
        Ok(Sqlite::DotCmd(cmd)) => run_dot_cmd(cmd, db),
        Ok(Sqlite::SqlStmt(stmt)) => run_sql_stmt(stmt, db),
        Err(e) => bail!("Invalid SQL: {}", e),
    }
}

fn run_dot_cmd(cmd: DotCmd, db: &Vec<u8>) -> Result<()> {
    match cmd {
        DotCmd::DbInfo => dbinfo(db),
        DotCmd::Tables => tables(db),
        DotCmd::Schema => schema(db),
    }
}

fn run_sql_stmt(stmt: SqlStmt, db: &Vec<u8>) -> Result<()> {
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

fn dbinfo(db: &Vec<u8>) -> Result<()> {
    let db_header = DbHeader::parse(&db[..100])?;
    println!("{:#?}", db_header);
    let schema = parse_db_schema(db)?;
    println!("number of tables: {}", schema.len());
    Ok(())
}

fn tables(db: &Vec<u8>) -> Result<()> {
    let names = parse_db_schema(db)?
        .into_iter()
        .map(|schema| schema.name)
        .collect::<Vec<_>>()
        .join(" ");
    println!("{}", names);
    Ok(())
}

fn schema(db: &Vec<u8>) -> Result<()> {
    fn get_sql(schema: Schema) -> Cow<str> {
        schema
            .sql
            .map(Cow::from)
            .unwrap_or_else(|| format!("[Object '{}' has no CREATE statement]", schema.name).into())
    }

    let sqls = parse_db_schema(db)?
        .into_iter()
        .map(get_sql)
        .collect::<Vec<_>>()
        .join("\n");

    println!("{}", sqls);

    Ok(())
}

fn count_rows(tbl: &str, db: &Vec<u8>) -> Result<()> {
    let page_size = DbHeader::parse(&db[..100])?.page_size;
    let tbl_schema = parse_db_schema(db)?
        .into_iter()
        .find(|x| x.type_ == "table" && x.name == tbl)
        .ok_or(anyhow!("Table '{}' not found", tbl))?;
    let page_offset = usize::try_from(tbl_schema.rootpage - 1)? * usize::from(page_size);
    let page_header = PageHeader::parse(&db[page_offset..page_offset + 12])?;
    println!("{}", page_header.number_of_cells);
    Ok(())
}

fn select_cols(result_cols: Vec<&str>, tbl: &str, db: &Vec<u8>) -> Result<()> {
    let page_size: usize = DbHeader::parse(db)?.page_size.into();

    let tbl_schema = parse_db_schema(db)?
        .into_iter()
        .find(|x| x.type_ == "table" && x.name == tbl)
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

    let page_offset = usize::try_from(tbl_schema.rootpage - 1)? * page_size;

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

    println!("{}", col_values);

    Ok(())
}

fn parse_db_schema(db: &[u8]) -> Result<Vec<Schema>> {
    let page_header = PageHeader::parse(&db[100..])?;

    db[100 + page_header.size()..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| usize::from(u16::from_be_bytes(bytes.try_into().unwrap())))
        .map(|cell_pointer| Cell::parse(&db[cell_pointer..]).and_then(Schema::parse))
        .collect::<Result<Vec<_>>>()
}
