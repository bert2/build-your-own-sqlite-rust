use anyhow::{anyhow, bail, Result};
use sqlite_starter_rust::{
    format::{cell::*, page::*, record::*},
    schema::*,
    str_sim::*,
    syntax::{ast::*, parser::*},
};
use std::{borrow::*, convert::*, env::args, fs::File, io::prelude::*};

fn main() -> Result<()> {
    let args = args().collect::<Vec<_>>();
    validate(&args)?;
    let db = read_db(&args[1])?;
    let output = parse_and_run(&args[2], &db)?;
    println!("{}", output);
    Ok(())
}

fn validate(args: &[String]) -> Result<()> {
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => Ok(()),
    }
}

fn read_db(file: &str) -> Result<Vec<u8>> {
    let mut file = File::open(file)?;
    let mut db = Vec::new();
    file.read_to_end(&mut db)?;
    Ok(db)
}

fn parse_and_run(sql: &str, db: &[u8]) -> Result<String> {
    match parse_sqlite(sql) {
        Ok(Sqlite::DotCmd(cmd)) => run_dot_cmd(cmd, db),
        Ok(Sqlite::SqlStmt(stmt)) => run_sql_stmt(stmt, db),
        Err(e) => bail!("Invalid SQL: {}", e),
    }
}

fn run_dot_cmd(cmd: DotCmd, db: &[u8]) -> Result<String> {
    match cmd {
        DotCmd::DbInfo => dbinfo(db),
        DotCmd::Tables => tables(db),
        DotCmd::Schema => schema(db),
    }
}

fn run_sql_stmt(stmt: SqlStmt, db: &[u8]) -> Result<String> {
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
                count_rows(tbl, db)
            } else {
                select_cols(&get_col_names(&cols)?, tbl, filter, db)
            }
        }
        _ => bail!("Not implemented: {:#?}", stmt),
    }
}

fn dbinfo(db: &[u8]) -> Result<String> {
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

fn tables(db: &[u8]) -> Result<String> {
    Ok(DbSchema::parse(db)?
        .tables()
        .map(|t| t.name)
        .collect::<Vec<_>>()
        .join(" "))
}

fn schema(db: &[u8]) -> Result<String> {
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

fn count_rows(tbl: &str, db: &[u8]) -> Result<String> {
    let (_, page) = load_tbl(tbl, db)?;
    Ok(format!("{}", page.header.number_of_cells))
}

#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(&'a [u8]),
    String(&'a str),
}

impl<'a> From<&ColContent<'a>> for Value<'a> {
    fn from(content: &ColContent<'a>) -> Self {
        match content {
            ColContent::Null => Value::Null,
            ColContent::Int8(_)
            | ColContent::Int16(_)
            | ColContent::Int24(_)
            | ColContent::Int32(_)
            | ColContent::Int48(_)
            | ColContent::Int64(_) => Value::Int(i64::try_from(content).unwrap()),
            ColContent::Float64(_) => Value::Float(f64::try_from(content).unwrap()),
            ColContent::False => Value::Bool(false),
            ColContent::True => Value::Bool(true),
            ColContent::Blob(bs) => Value::Bytes(bs),
            ColContent::Text(_) => Value::String(<&str>::try_from(content).unwrap()),
        }
    }
}

trait Eval<'a> {
    fn eval(&self, c: &Cell<'a>, s: &Schema<'a>) -> Value<'a>;
}

impl<'a> Eval<'a> for Expr<'a> {
    fn eval(&self, c: &Cell<'a>, s: &Schema<'a>) -> Value<'a> {
        match self {
            Expr::Null => Value::Null,
            Expr::String(s) => Value::String(s),
            Expr::Int(i) => Value::Int(*i),
            Expr::ColName(col) => {
                if s.cols().is_int_pk(col) {
                    Value::Int(c.row_id)
                } else {
                    (&c.payload[s.cols().index(col)]).into()
                }
            }
        }
    }
}

impl<'a> Eval<'a> for BoolExpr<'a> {
    fn eval(&self, c: &Cell<'a>, s: &Schema<'a>) -> Value<'a> {
        match self {
            BoolExpr::Equals { l, r } => Value::Bool(l.eval(c, s) == r.eval(c, s)),
            BoolExpr::NotEquals { l, r } => Value::Bool(l.eval(c, s) != r.eval(c, s)),
        }
    }
}

fn select_cols(
    result_cols: &[&str],
    tbl: &str,
    filter: Option<BoolExpr>,
    db: &[u8],
) -> Result<String> {
    let (schema, page) = load_tbl(tbl, db)?;

    result_cols.iter().try_for_each(|col| {
        if !schema.cols().has(col) {
            bail!(
                "Unknown column '{}'. Did you mean '{}'?",
                col,
                most_similar(col, &schema.cols().names()).unwrap()
            )
        } else {
            Ok(())
        }
    })?;

    Ok(page
        .cells()?
        .iter()
        .filter(|cell| match &filter {
            Some(expr) => {
                if let Value::Bool(b) = expr.eval(cell, &schema) {
                    b
                } else {
                    panic!("omg")
                }
            }
            None => true,
        })
        .map(|cell| {
            result_cols
                .iter()
                .map(|res_col| {
                    if schema.cols().is_int_pk(res_col) {
                        cell.row_id.to_string()
                    } else {
                        format!("{}", cell.payload[schema.cols().index(res_col)])
                    }
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

fn load_tbl<'a>(tbl: &str, db: &'a [u8]) -> Result<(Schema<'a>, Page<'a>)> {
    let db_schema = DbSchema::parse(db)?;
    let page_size = db_schema.db_header.page_size.into();
    let schema = db_schema
        .table(tbl)
        .ok_or(anyhow!("Table '{}' not found", tbl))?;
    let page = Page::parse(schema.rootpage, page_size, db)?;

    Ok((schema, page))
}
