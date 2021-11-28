use anyhow::{anyhow, Result};
use nom::{
    branch::*, bytes::complete::*, character::complete::*, combinator::*, error::*, multi::*,
    sequence::*, Finish, IResult, Parser,
};

type R<'a, O> = IResult<&'a str, O, VerboseError<&'a str>>;

#[derive(Debug, Clone)]
pub enum Sqlite {
    DotCmd(DotCmd),
    SqlStmt(SqlStmt),
}

#[derive(Debug, Clone)]
pub enum DotCmd {
    DbInfo,
    Tables,
    Schema,
}

#[derive(Debug, Clone)]
pub enum SqlStmt {
    CreateTbl {
        tbl_name: String,
        col_names: Vec<String>,
    },
    Select {
        col: Expr,
        tbl: String,
    },
}

#[derive(Debug, Clone)]
pub enum Expr {
    ColName(String),
    Count,
}

fn skip<'a, O, E: ParseError<&'a str>, P: Parser<&'a str, O, E>>(
    p: P,
) -> impl Parser<&'a str, (), E> {
    value((), p)
}

fn ws<'a, O, E: ParseError<&'a str>, F: Parser<&'a str, O, E>>(f: F) -> impl Parser<&'a str, O, E> {
    delimited(multispace0, f, multispace0)
}

fn dot_cmd(i: &str) -> R<Sqlite> {
    delimited(
        char('.'),
        alt((
            value(DotCmd::DbInfo, tag("dbinfo")),
            value(DotCmd::Tables, tag("tables")),
            value(DotCmd::Schema, tag("schema")),
        )),
        multispace0,
    )
    .map(Sqlite::DotCmd)
    .parse(i)
}

fn select_start(i: &str) -> R<()> {
    skip(delimited(multispace0, tag_no_case("SELECT"), multispace1)).parse(i)
}

fn select_col(i: &str) -> R<Expr> {
    terminated(
        alt((
            value(Expr::Count, tag_no_case("COUNT(*)")),
            alpha1.map(|name: &str| Expr::ColName(name.to_string())),
        )),
        multispace1,
    )(i)
}

fn select_tbl_name(i: &str) -> R<&str> {
    tuple((tag_no_case("FROM"), multispace1, alpha1, multispace0))
        .map(|x| x.2)
        .parse(i)
}

fn select_stmt(i: &str) -> R<Sqlite> {
    tuple((select_start, select_col, select_tbl_name))
        .map(|x| SqlStmt::Select {
            col: x.1,
            tbl: x.2.to_string(),
        })
        .map(Sqlite::SqlStmt)
        .parse(i)
}

fn create_tbl_start(i: &str) -> R<()> {
    skip(tuple((
        multispace0,
        tag_no_case("CREATE"),
        multispace1,
        tag_no_case("TABLE"),
        multispace1,
    )))
    .parse(i)
}

fn create_tbl_id(i: &str) -> R<&str> {
    terminated(alpha1, multispace0)(i)
}

fn create_tbl_coldef(i: &str) -> R<&str> {
    terminated(alpha1, take_while(|c| c != ',' && c != ')'))(i)
}

fn create_tbl_coldefs(i: &str) -> R<Vec<&str>> {
    delimited(
        terminated(char('('), multispace0),
        separated_list1(ws(char(',')), create_tbl_coldef),
        terminated(char(')'), multispace0),
    )(i)
}

fn create_tbl_stmt(i: &str) -> R<Sqlite> {
    tuple((
        create_tbl_start,
        create_tbl_id,
        create_tbl_coldefs,
        multispace0,
    ))
    .map(|x| SqlStmt::CreateTbl {
        tbl_name: x.1.to_string(),
        col_names: x.2.iter().map(|n| n.to_string()).collect(),
    })
    .map(Sqlite::SqlStmt)
    .parse(i)
}

pub fn parse(sql: &str) -> Result<Sqlite> {
    let mut p = alt((dot_cmd, create_tbl_stmt, select_stmt));

    p.parse(sql)
        .finish()
        .map(|r| r.1)
        .map_err(|e| anyhow!(convert_error(sql, e)))
}
