use anyhow::{anyhow, bail, Result};
use sqlite_starter_rust::{
    db_header::DbHeader, page_header::PageHeader, record::parse_record, schema, schema::Schema,
    sql::*, varint::parse_varint,
};
use std::convert::TryInto;
use std::env::args;
use std::fs::File;
use std::io::prelude::*;

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

fn parse_and_run(cmd: &str, db: &Vec<u8>) -> Result<()> {
    match parse(cmd) {
        Ok(Sqlite::DotCmd(DotCmd::DbInfo)) => dbinfo(db),
        Ok(Sqlite::DotCmd(DotCmd::Tables)) => tables(db),
        Ok(Sqlite::DotCmd(DotCmd::Schema)) => schema(db),
        Ok(Sqlite::SqlStmt(SqlStmt::Select {
            col: Expr::Count,
            tbl,
        })) => count_rows(tbl, db),
        Err(e) => bail!("Invalid query: {}", e),
        x => bail!("not implemented: {:#?}", x),
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
    let sqls = parse_db_schema(db)?
        .into_iter()
        .map(|schema| schema.sql)
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
    let page_offset = ((tbl_schema.rootpage - 1) as usize) * (page_size as usize);
    let page_header = PageHeader::parse(&db[page_offset..page_offset + 12])?;
    //println!("{:#?}", page_header);
    println!("{}", page_header.number_of_cells);
    Ok(())
}

fn parse_db_schema(db: &Vec<u8>) -> Result<Vec<Schema>> {
    let page_header = PageHeader::parse(&db[100..112])?;

    // Obtain all cell pointers
    let cell_pointers = db[100 + page_header.size()..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();

    // Obtain all schema records
    cell_pointers
        .into_iter()
        .map(|cell_pointer| {
            let mut offset = cell_pointer as usize;
            offset += parse_varint(&db[offset..]).1; // payload size
            offset += parse_varint(&db[offset..]).1; // row id
            parse_record(&db[offset..], schema::COLUMN_COUNT)
                .map(|record| Schema::parse(record).expect("Invalid record"))
        })
        .collect::<Result<Vec<_>>>()
}
