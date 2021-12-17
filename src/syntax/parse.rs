use crate::syntax::ast::*;
use anyhow::{anyhow, Result};
use nom::{error::*, Finish};

pub fn sqlite(sql: &str) -> Result<Sqlite> {
    parsers::sqlite(sql)
        .finish()
        .map(|r| r.1)
        .map_err(|e| anyhow!(convert_error(sql, e)))
}

pub fn sql_stmt(sql: &str) -> Result<SqlStmt> {
    parsers::sql_stmt(sql)
        .finish()
        .map(|r| r.1)
        .map_err(|e| anyhow!(convert_error(sql, e)))
}

mod parsers {
    use crate::syntax::{ast::*, util::*};
    use nom::{
        branch::*, bytes::complete::*, character::complete::*, combinator::*, error::*,
        multi::many0, sequence::*, IResult, Parser,
    };

    type R<'a, O> = IResult<&'a str, O, VerboseError<&'a str>>;

    pub fn sqlite(i: &str) -> R<Sqlite> {
        terminated(
            alt((dot_cmd.map(Sqlite::DotCmd), sql_stmt.map(Sqlite::SqlStmt))),
            eof,
        )(i)
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

    pub fn sql_stmt(i: &str) -> R<SqlStmt> {
        terminated(alt((create_tbl_stmt, select_stmt)), eof)(i)
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

    fn create_tbl_coldef(i: &str) -> R<ColDef> {
        let int_pk_col = tuple((
            identifier,
            skip(preceded_ws1(tag_no_case("INTEGER"))),
            skip(preceded_ws1(tag_no_case("PRIMARY"))),
            skip(preceded_ws1(tag_no_case("KEY"))),
            skip(take_while(|c| c != ',' && c != ')')),
        ))
        .map(|x| ColDef::IntPk(x.0));

        let other_col =
            terminated(identifier, take_while(|c| c != ',' && c != ')')).map(ColDef::Col);

        alt((int_pk_col, other_col))(i)
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
            delimited_ws0(alt((tag("=="), tag("="), tag("!="), tag("<>")))),
            expr,
        ))
        .map(|x| match x.2 {
            "==" | "=" => BoolExpr::Equals { l: x.1, r: x.3 },
            "!=" | "<>" => BoolExpr::NotEquals { l: x.1, r: x.3 },
            _ => panic!("Unsupported operator {}", x.2),
        })
        .parse(i)
    }

    fn expr(i: &str) -> R<Expr> {
        alt((
            value(Expr::Null, tag_no_case("NULL")),
            str_lit.map(Expr::String),
            num.map(Expr::Int),
            identifier.map(Expr::ColName),
        ))(i)
    }

    fn identifier(i: &str) -> R<&str> {
        alt((delimited_identifier, regular_identifier))(i)
    }

    fn regular_identifier(i: &str) -> R<&str> {
        recognize(pair(
            alt((alpha1, tag("_"))),
            many0(alt((alphanumeric1, tag("_")))),
        ))(i)
    }

    fn delimited_identifier(i: &str) -> R<&str> {
        delimited(char('"'), is_not("\""), char('"'))(i)
    }

    fn str_lit(i: &str) -> R<&str> {
        delimited(char('\''), is_not("'"), char('\''))(i)
    }

    fn num(i: &str) -> R<i64> {
        map_res(digit1, str::parse)(i)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn select_single_col() {
        assert_eq!(
            sql_stmt("select foo from bar").unwrap(),
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
            sql_stmt("select foo, bar, qux from my_tbl").unwrap(),
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
    fn select_delimited_table_name() {
        assert_eq!(
            sql_stmt("select foo from \"my table!\"").unwrap(),
            SqlStmt::Select {
                cols: vec![ResultExpr::Value(Expr::ColName("foo"))],
                tbl: "my table!",
                filter: None
            }
        )
    }

    #[test]
    fn select_with_filter() {
        assert_eq!(
            sql_stmt("select foo from bar where qux = 'my filter'").unwrap(),
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
