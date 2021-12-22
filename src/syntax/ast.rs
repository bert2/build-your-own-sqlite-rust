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
    Select(Select<'a>),
}

#[derive(Debug, PartialEq)]
pub struct Select<'a> {
    pub cols: Vec<Expr<'a>>,
    pub tbl: &'a str,
    pub filter: Option<BoolExpr<'a>>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Expr<'a> {
    Literal(Literal<'a>),
    ColName(&'a str),
    Count,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Literal<'a> {
    Null,
    String(&'a str),
    Int(i64),
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

impl<'a> Select<'a> {
    pub fn selected_col_names(&self) -> impl Iterator<Item = &str> {
        self.cols.iter().filter_map(Expr::as_col_name)
    }

    pub fn has_count_expr(&self) -> bool {
        self.cols.iter().any(|c| matches!(c, Expr::Count))
    }
}

impl<'a> BoolExpr<'a> {
    pub fn referenced_col_names(&self) -> impl Iterator<Item = &str> {
        match self {
            BoolExpr::Equals { l, r } | BoolExpr::NotEquals { l, r } => l
                .as_col_name()
                .into_iter()
                .chain(r.as_col_name().into_iter()),
        }
    }

    pub fn is_int_pk_servable(&self) -> Option<(&str, i64)> {
        match self {
            BoolExpr::Equals {
                l: Expr::ColName(c),
                r: Expr::Literal(Literal::Int(pk)),
            }
            | BoolExpr::Equals {
                l: Expr::Literal(Literal::Int(pk)),
                r: Expr::ColName(c),
            } => Some((c, *pk)),
            _ => None,
        }
    }

    pub fn is_index_servable(&self) -> Option<(&str, &Literal)> {
        match self {
            BoolExpr::Equals {
                l: Expr::ColName(c),
                r: Expr::Literal(literal),
            }
            | BoolExpr::Equals {
                l: Expr::Literal(literal),
                r: Expr::ColName(c),
            } => Some((c, literal)),
            _ => None,
        }
    }
}
