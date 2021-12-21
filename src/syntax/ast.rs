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
        name: &'a str,
        col_defs: Vec<ColDef<'a>>,
    },
    CreateIdx {
        name: &'a str,
        target_tbl: &'a str,
        target_col: &'a str,
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
    pub const fn is_int_pk(self: &&ColDef<'a>) -> bool {
        matches!(self, ColDef::IntPk(_))
    }

    pub const fn name(&self) -> &'a str {
        match self {
            ColDef::IntPk(n) | ColDef::Col(n) => *n,
        }
    }
}

impl<'a> Expr<'a> {
    pub fn as_col_name(&self) -> Option<&str> {
        match self {
            Expr::ColName(c) => Some(c),
            _ => None,
        }
    }
}

impl<'a> BoolExpr<'a> {
    pub fn referenced_cols(&self) -> impl Iterator<Item = &str> {
        match self {
            BoolExpr::Equals { l, r } | BoolExpr::NotEquals { l, r } => l
                .as_col_name()
                .into_iter()
                .chain(r.as_col_name().into_iter()),
        }
    }

    pub fn int_pk_servable(&self) -> Option<(&str, i64)> {
        match self {
            BoolExpr::Equals {
                l: Expr::ColName(c),
                r: Expr::Int(pk),
            }
            | BoolExpr::Equals {
                l: Expr::Int(pk),
                r: Expr::ColName(c),
            } => Some((c, *pk)),
            _ => None,
        }
    }

    pub fn index_searchable_col(&self) -> Option<&str> {
        match self {
            BoolExpr::Equals {
                l: Expr::ColName(c),
                r: other,
            }
            | BoolExpr::Equals {
                l: other,
                r: Expr::ColName(c),
            } if !matches!(other, Expr::ColName(_)) => Some(c),
            _ => None,
        }
    }
}
