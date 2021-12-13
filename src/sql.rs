use crate::nom_helpers::*;
use anyhow::{anyhow, Result};
use nom::{
    branch::*, bytes::complete::*, character::complete::*, combinator::*, error::*, multi::many0,
    sequence::*, Finish, IResult, Parser,
};

#[derive(Debug, PartialEq)]
pub enum Sqlite<'a> {
    DotCmd(DotCmd),
    SqlStmt(SqlStmt<'a>),
}

#[derive(Debug, PartialEq, Clone)]
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

#[derive(Debug, PartialEq)]
pub enum SqlStmt<'a> {
    CreateTbl {
        tbl_name: &'a str,
        col_defs: Vec<ColDef<'a>>,
    },
    Select {
        cols: Vec<ResultExpr<'a>>,
        tbl: &'a str,
        filter: Option<BoolExpr<'a>>,
    },
}

#[derive(Debug, PartialEq, Clone)]
pub enum ResultExpr<'a> {
    Count,
    Value(Expr<'a>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expr<'a> {
    Null,
    String(&'a str),
    Int(i64),
    ColName(&'a str),
}

#[derive(Debug, PartialEq)]
pub enum BoolExpr<'a> {
    Equals { l: Expr<'a>, r: Expr<'a> },
}

type R<'a, O> = IResult<&'a str, O, VerboseError<&'a str>>;

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

fn str_lit(i: &str) -> R<&str> {
    delimited(char('\''), is_not("'"), char('\''))(i)
}

fn num(i: &str) -> R<i64> {
    map_res(digit1, str::parse)(i)
}

pub fn identifier(i: &str) -> R<&str> {
    recognize(pair(
        alt((alpha1, tag("_"))),
        many0(alt((alphanumeric1, tag("_")))),
    ))(i)
}

fn expr(i: &str) -> R<Expr> {
    alt((
        value(Expr::Null, tag_no_case("NULL")),
        str_lit.map(Expr::String),
        num.map(Expr::Int),
        identifier.map(Expr::ColName),
    ))(i)
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

fn select_result_cols(i: &str) -> R<Vec<ResultExpr>> {
    comma_separated_list1(alt((
        value(ResultExpr::Count, tag_no_case("COUNT(*)")),
        identifier.map(Expr::ColName).map(ResultExpr::Value),
    )))
    .parse(i)
}

fn select_filter(i: &str) -> R<BoolExpr> {
    tuple((
        skip(delimited_ws1(tag_no_case("WHERE"))),
        expr,
        skip(delimited_ws0(char('='))),
        expr,
    ))
    .map(|x| BoolExpr::Equals { l: x.1, r: x.3 })
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
        opt(select_filter),
        skip(multispace0),
    ))
    .map(|x| SqlStmt::Select {
        cols: x.3,
        tbl: x.5,
        filter: x.6,
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
    terminated(alt((select_stmt, create_tbl_stmt)), eof)(i)
}

fn sqlite(i: &str) -> R<Sqlite> {
    terminated(
        alt((dot_cmd.map(Sqlite::DotCmd), sql_stmt.map(Sqlite::SqlStmt))),
        eof,
    )(i)
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn select_single_col() {
        assert_eq!(
            parse_sql_stmt("select foo from bar").unwrap(),
            SqlStmt::Select {
                cols: vec![ResultExpr::Value(Expr::ColName("foo"))],
                tbl: "bar",
                filter: None
            }
        )
    }

    #[test]
    fn select_multiple_cols() {
        assert_eq!(
            parse_sql_stmt("select foo, bar, qux from my_tbl").unwrap(),
            SqlStmt::Select {
                cols: vec![
                    ResultExpr::Value(Expr::ColName("foo")),
                    ResultExpr::Value(Expr::ColName("bar")),
                    ResultExpr::Value(Expr::ColName("qux"))
                ],
                tbl: "my_tbl",
                filter: None
            }
        )
    }

    #[test]
    fn select_with_filter() {
        assert_eq!(
            parse_sql_stmt("select foo from bar where qux = 'my filter'").unwrap(),
            SqlStmt::Select {
                cols: vec![ResultExpr::Value(Expr::ColName("foo"))],
                tbl: "bar",
                filter: Some(BoolExpr::Equals {
                    l: Expr::ColName("qux"),
                    r: Expr::String("my filter")
                })
            }
        )
    }
}
