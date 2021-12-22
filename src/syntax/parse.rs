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
        terminated(alt((create_idx_stmt, create_tbl_stmt, select_stmt)), eof)(i)
    }

    fn create_idx_stmt(i: &str) -> R<SqlStmt> {
        tuple((
            skip(preceded_ws0(tag_no_case("CREATE"))),
            skip(preceded_ws1(tag_no_case("INDEX"))),
            skip(opt(preceded_ws1(if_not_exists_clause))),
            preceded_ws1(identifier),
            skip(preceded_ws1(tag_no_case("ON"))),
            preceded_ws1(identifier),
            skip(delimited_ws0(char('('))),
            identifier,
            skip(terminated_ws0(char(')'))),
        ))
        .map(|x| SqlStmt::CreateIdx {
            name: x.3,
            target_tbl: x.5,
            target_col: x.7,
        })
        .parse(i)
    }

    fn create_tbl_stmt(i: &str) -> R<SqlStmt> {
        tuple((
            skip(preceded_ws0(tag_no_case("CREATE"))),
            skip(preceded_ws1(tag_no_case("TABLE"))),
            skip(opt(preceded_ws1(if_not_exists_clause))),
            preceded_ws1(identifier),
            skip(delimited_ws0(char('('))),
            comma_separated_list1(create_tbl_coldef),
            skip(terminated_ws0(char(')'))),
        ))
        .map(|x| SqlStmt::CreateTbl {
            name: x.3,
            col_defs: x.5,
        })
        .parse(i)
    }

    fn create_tbl_coldef(i: &str) -> R<ColDef> {
        let int_pk_col = tuple((
            identifier,
            skip(preceded_ws1(tag_no_case("INTEGER"))),
            skip(preceded_ws1(tag_no_case("PRIMARY"))),
            skip(preceded_ws1(tag_no_case("KEY"))),
            skip_col_constraints,
        ))
        .map(|x| ColDef::IntPk(x.0));

        let other_col = terminated(identifier, skip_col_constraints).map(ColDef::Col);

        alt((int_pk_col, other_col))(i)
    }

    fn skip_col_constraints(i: &str) -> R<()> {
        skip(alt((
            pair(multispace0, peek(is_a(",)"))),
            pair(multispace1, take_while(|c| c != ',' && c != ')')),
        )))
        .parse(i)
    }

    fn if_not_exists_clause(i: &str) -> R<()> {
        skip(tuple((
            tag_no_case("IF"),
            preceded_ws1(tag_no_case("NOT")),
            preceded_ws1(tag_no_case("EXISTS")),
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
            opt(select_filter),
            skip(multispace0),
        ))
        .map(|x| {
            SqlStmt::Select(Select {
                cols: x.3,
                tbl: x.5,
                filter: x.6,
            })
        })
        .parse(i)
    }

    fn select_result_cols(i: &str) -> R<Vec<Expr>> {
        comma_separated_list1(alt((value(Expr::Count, tag_no_case("COUNT(*)")), expr))).parse(i)
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

    fn lit(i: &str) -> R<Literal> {
        alt((
            value(Literal::Null, tag_no_case("NULL")),
            str_lit.map(Literal::String),
            num.map(Literal::Int),
        ))(i)
    }

    fn expr(i: &str) -> R<Expr> {
        alt((lit.map(Expr::Literal), identifier.map(Expr::ColName)))(i)
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
    mod create_tbl {
        use super::super::*;

        #[test]
        fn basic() {
            assert_eq!(
                sql_stmt("create table foo (bar, qux)").unwrap(),
                SqlStmt::CreateTbl {
                    name: "foo",
                    col_defs: vec![ColDef::Col("bar"), ColDef::Col("qux")],
                }
            )
        }

        #[test]
        fn ignores_types_and_constraints_on_cols() {
            assert_eq!(
                sql_stmt("create table foo (bar text default 'bar', qux blob unique not null)")
                    .unwrap(),
                SqlStmt::CreateTbl {
                    name: "foo",
                    col_defs: vec![ColDef::Col("bar"), ColDef::Col("qux")],
                }
            )
        }

        #[test]
        fn captures_int_pk_constraint() {
            assert_eq!(
                sql_stmt("create table foo (bar integer primary key, qux)").unwrap(),
                SqlStmt::CreateTbl {
                    name: "foo",
                    col_defs: vec![ColDef::IntPk("bar"), ColDef::Col("qux")],
                }
            )
        }

        #[test]
        fn ignores_exists_clause() {
            assert_eq!(
                sql_stmt("create table if not exists foo (bar, qux)").unwrap(),
                SqlStmt::CreateTbl {
                    name: "foo",
                    col_defs: vec![ColDef::Col("bar"), ColDef::Col("qux")],
                }
            )
        }

        #[test]
        fn delimited_identifiers() {
            assert_eq!(
                sql_stmt("create table \"my tbl!\" (\"my col!\")").unwrap(),
                SqlStmt::CreateTbl {
                    name: "my tbl!",
                    col_defs: vec![ColDef::Col("my col!")],
                }
            )
        }
    }

    mod create_idx {
        use super::super::*;

        #[test]
        fn basic() {
            assert_eq!(
                sql_stmt("create index foo on bar (qux)").unwrap(),
                SqlStmt::CreateIdx {
                    name: "foo",
                    target_tbl: "bar",
                    target_col: "qux",
                }
            )
        }

        #[test]
        fn ignores_exists_clause() {
            assert_eq!(
                sql_stmt("create index if not exists foo on bar (qux)").unwrap(),
                SqlStmt::CreateIdx {
                    name: "foo",
                    target_tbl: "bar",
                    target_col: "qux",
                }
            )
        }

        #[test]
        fn delimited_identifiers() {
            assert_eq!(
                sql_stmt("create index \"my idx!\" on \"my tbl!\" (\"my col!\")").unwrap(),
                SqlStmt::CreateIdx {
                    name: "my idx!",
                    target_tbl: "my tbl!",
                    target_col: "my col!",
                }
            )
        }
    }

    mod select {
        use super::super::*;

        #[test]
        fn single_col() {
            assert_eq!(
                sql_stmt("select foo from bar").unwrap(),
                SqlStmt::Select(Select {
                    cols: vec![Expr::ColName("foo")],
                    tbl: "bar",
                    filter: None
                })
            )
        }

        #[test]
        fn count() {
            assert_eq!(
                sql_stmt("select count(*) from bar").unwrap(),
                SqlStmt::Select(Select {
                    cols: vec![Expr::Count],
                    tbl: "bar",
                    filter: None
                })
            )
        }

        #[test]
        fn multiple_cols() {
            assert_eq!(
                sql_stmt("select foo, bar, qux from my_tbl").unwrap(),
                SqlStmt::Select(Select {
                    cols: vec![
                        Expr::ColName("foo"),
                        Expr::ColName("bar"),
                        Expr::ColName("qux")
                    ],
                    tbl: "my_tbl",
                    filter: None
                })
            )
        }

        #[test]
        fn delimited_table_name() {
            assert_eq!(
                sql_stmt("select foo from \"my tbl!\"").unwrap(),
                SqlStmt::Select(Select {
                    cols: vec![Expr::ColName("foo")],
                    tbl: "my tbl!",
                    filter: None
                })
            )
        }

        #[test]
        fn with_filter() {
            assert_eq!(
                sql_stmt("select foo from bar where qux = 'my filter'").unwrap(),
                SqlStmt::Select(Select {
                    cols: vec![Expr::ColName("foo")],
                    tbl: "bar",
                    filter: Some(BoolExpr::Equals {
                        l: Expr::ColName("qux"),
                        r: Expr::Literal(Literal::String("my filter"))
                    })
                })
            )
        }
    }
}
