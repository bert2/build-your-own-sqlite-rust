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
    NotEquals { l: Expr<'a>, r: Expr<'a> },
}

impl<'a> ColDef<'a> {
    pub fn is_int_pk(self: &&ColDef<'a>) -> bool {
        matches!(self, ColDef::IntPk(_))
    }

    pub fn name(&self) -> &'a str {
        match self {
            ColDef::IntPk(n) => *n,
            ColDef::Col(n) => *n,
        }
    }
}
