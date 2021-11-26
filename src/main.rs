use anyhow::{anyhow, bail, Result};
use sqlite_starter_rust::{
    db_header::DbHeader, page_header::PageHeader, record::parse_record, schema::Schema,
    varint::parse_varint,
};
use std::convert::TryInto;
use std::env::args;
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<()> {
    let args = validate(args().collect::<Vec<_>>())?;
    let mut db = read_db(&args[1])?;
    parse_and_run(&args[2], &mut db)
}

fn validate(args: Vec<String>) -> Result<Vec<String>> {
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => Ok(args),
    }
}

fn read_db(file: &String) -> Result<Vec<u8>> {
    let mut file = File::open(file)?;
    let mut db = Vec::new();
    file.read_to_end(&mut db)?;
    Ok(db)
}

fn parse_and_run(cmd: &String, db: &mut Vec<u8>) -> Result<()> {
    fn is_count_query(cmd: &String) -> bool {
        cmd.to_uppercase().starts_with("SELECT COUNT(*) FROM ")
    }

    match cmd.as_str() {
        ".dbinfo" => dbinfo(db),
        ".tables" => tables(db),
        _ if is_count_query(cmd) => {
            let tbl = cmd.split(' ').last().unwrap();
            count_rows(tbl, db)
        }
        _ => bail!("Invalid query: {}", cmd),
    }
}

fn dbinfo(db: &mut Vec<u8>) -> Result<()> {
    let db_header = DbHeader::parse(&db[..100])?;
    println!("{:#?}", db_header);
    let schema = parse_db_schema(db)?;
    println!("number of tables: {}", schema.len());
    Ok(())
}

fn tables(db: &mut Vec<u8>) -> Result<()> {
    let tbls = parse_db_schema(db)?
        .into_iter()
        .map(|schema| schema.name)
        .collect::<Vec<_>>()
        .join(" ");
    println!("{}", tbls);
    Ok(())
}

fn count_rows(tbl: &str, db: &mut Vec<u8>) -> Result<()> {
    let page_size = DbHeader::parse(&db[..100])?.page_size;
    let tbl_schema = parse_db_schema(db)?
        .into_iter()
        .find(|x| x.type_ == "table" && x.name == tbl)
        .ok_or(anyhow!("Table '{}' not found", tbl))?;
    let page_offset = ((tbl_schema.rootpage - 1) as usize) * (page_size as usize);
    let page_header = PageHeader::parse(&db[page_offset..page_offset + 8])?;
    println!("{}", page_header.number_of_cells);
    Ok(())
}

fn parse_db_schema(db: &mut Vec<u8>) -> Result<Vec<Schema>> {
    // Parse page header from database
    let page_header = PageHeader::parse(&db[100..108])?;

    // Obtain all cell pointers
    let cell_pointers = db[108..]
        .chunks_exact(2)
        .take(page_header.number_of_cells.into())
        .map(|bytes| u16::from_be_bytes(bytes.try_into().unwrap()))
        .collect::<Vec<_>>();

    // Obtain all records from column 5
    cell_pointers
        .into_iter()
        .map(|cell_pointer| {
            let stream = &db[cell_pointer as usize..];
            let (_, offset) = parse_varint(stream);
            let (_rowid, read_bytes) = parse_varint(&stream[offset..]);
            parse_record(&stream[offset + read_bytes..], 5)
                .map(|record| Schema::parse(record).expect("Invalid record"))
        })
        .collect::<Result<Vec<_>>>()
}
