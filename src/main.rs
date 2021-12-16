use anyhow::{anyhow, bail, Result};
use sqlite_starter_rust::{interpreter::exec, syntax::parse};
use std::{env::args, fs::File, io::prelude::Read};

fn main() -> Result<()> {
    let args = args().collect::<Vec<_>>();
    let (db_file, sql) = parse_args(&args)?;

    let db = read_db(db_file)?;

    let sql = parse::sqlite(sql).map_err(|e| anyhow!("Invalid SQL: {}", e))?;

    let output = exec::sqlite(sql, &db)?;
    println!("{}", output);

    Ok(())
}

fn parse_args(args: &[String]) -> Result<(&str, &str)> {
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => Ok((&args[1], &args[2])),
    }
}

fn read_db(file: &str) -> Result<Vec<u8>> {
    let mut file = File::open(file)?;
    let mut db = Vec::new();
    file.read_to_end(&mut db)?;
    Ok(db)
}
