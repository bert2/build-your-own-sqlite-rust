use crate::nom_helpers::*;
use anyhow::{anyhow, Result};
use nom::{
    branch::*, bytes::complete::*, character::complete::*, combinator::*, error::*, sequence::*,
    Finish, IResult, Parser,
};

#[derive(Debug)]
pub enum Sqlite<'a> {
    DotCmd(DotCmd),
    SqlStmt(SqlStmt<'a>),
}

#[derive(Debug, Clone)]
pub enum DotCmd {
    DbInfo,
    Tables,
    Schema,
}

#[derive(Debug, PartialEq)]
pub enum ColDef<'a> {
    IntPk(&'a str),
    Col(&'a str),
}

impl<'a> ColDef<'a> {
    pub fn is_int_pk(self: &&ColDef<'a>) -> bool {
        match self {
            ColDef::IntPk(_) => true,
            _ => false,
        }
    }

    pub fn name(&self) -> &'a str {
        match self {
            ColDef::IntPk(n) => *n,
            ColDef::Col(n) => *n,
        }
    }
}

#[derive(Debug)]
pub enum SqlStmt<'a> {
    CreateTbl {
        tbl_name: &'a str,
        col_defs: Vec<ColDef<'a>>,
    },
    Select {
        cols: Vec<Expr<'a>>,
        tbl: &'a str,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr<'a> {
    ColName(&'a str),
    Count,
}

type R<'a, O> = IResult<&'a str, O, VerboseError<&'a str>>;

fn identifier(i: &str) -> R<&str> {
    alpha1(i)
}

fn dot_cmd(i: &str) -> R<DotCmd> {
    delimited(
        char('.'),
        alt((
            value(DotCmd::DbInfo, tag("dbinfo")),
            value(DotCmd::Tables, tag("tables")),
            value(DotCmd::Schema, tag("schema")),
        )),
        multispace0,
    )(i)
}

fn select_result_cols(i: &str) -> R<Vec<Expr>> {
    comma_separated_list1(alt((
        value(Expr::Count, tag_no_case("COUNT(*)")),
        identifier.map(Expr::ColName),
    )))
    .parse(i)
}

fn select_stmt(i: &str) -> R<SqlStmt> {
    tuple((
        skip(multispace0),
        skip(tag_no_case("SELECT")),
        skip(multispace1),
        select_result_cols,
        skip(delimited_ws1(tag_no_case("FROM"))),
        identifier,
        skip(multispace0),
    ))
    .map(|x| SqlStmt::Select {
        cols: x.3,
        tbl: x.5,
    })
    .parse(i)
}

fn create_tbl_coldef(i: &str) -> R<ColDef> {
    let int_pk_col = tuple((
        identifier,
        skip(preceded_ws1(tag_no_case("INTEGER"))),
        skip(preceded_ws1(tag_no_case("PRIMARY"))),
        skip(preceded_ws1(tag_no_case("KEY"))),
        skip(take_while(|c| c != ',' && c != ')')),
    ))
    .map(|x| ColDef::IntPk(x.0));

    let other_col = terminated(identifier, take_while(|c| c != ',' && c != ')')).map(ColDef::Col);

    alt((int_pk_col, other_col))(i)
}

fn create_tbl_stmt(i: &str) -> R<SqlStmt> {
    tuple((
        skip(preceded_ws0(tag_no_case("CREATE"))),
        skip(delimited_ws1(tag_no_case("TABLE"))),
        identifier,
        skip(delimited_ws0(char('('))),
        comma_separated_list1(create_tbl_coldef),
        skip(terminated_ws0(char(')'))),
    ))
    .map(|x| SqlStmt::CreateTbl {
        tbl_name: x.2,
        col_defs: x.4,
    })
    .parse(i)
}

fn sql_stmt(i: &str) -> R<SqlStmt> {
    alt((select_stmt, create_tbl_stmt))(i)
}

fn sqlite(i: &str) -> R<Sqlite> {
    alt((dot_cmd.map(Sqlite::DotCmd), sql_stmt.map(Sqlite::SqlStmt)))(i)
}

pub fn parse_sqlite(sql: &str) -> Result<Sqlite> {
    sqlite(sql)
        .finish()
        .map(|r| r.1)
        .map_err(|e| anyhow!(convert_error(sql, e)))
}

pub fn parse_sql_stmt(sql: &str) -> Result<SqlStmt> {
    sql_stmt(sql)
        .finish()
        .map(|r| r.1)
        .map_err(|e| anyhow!(convert_error(sql, e)))
}
