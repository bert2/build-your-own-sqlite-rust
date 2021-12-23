use crate::{
    syntax::{parse, ColDef, SqlStmt},
    util::{flip, IterEither},
};
use anyhow::{anyhow, bail, Result};
use std::{collections::HashMap, iter::once};

#[derive(Debug)]
pub enum Cols<'a> {
    TblCols {
        int_pk: Option<&'a str>,
        name_to_pos: HashMap<&'a str, usize>,
    },
    IdxCol(&'a str),
}

impl<'a> Cols<'a> {
    pub fn parse(create_sql: &'a str) -> Result<Self> {
        let sql = parse::sql_stmt(create_sql)
            .map_err(|e| anyhow!("Failed to parse CREATE statement: {}", e))?;

        Ok(match sql {
            SqlStmt::CreateTbl { col_defs, .. } => Self::TblCols {
                int_pk: col_defs.iter().find(ColDef::is_int_pk).map(ColDef::name),
                name_to_pos: col_defs
                    .iter()
                    .map(ColDef::name)
                    .enumerate()
                    .map(flip)
                    .collect::<HashMap<_, _>>(),
            },
            SqlStmt::CreateIdx { target_col, .. } => Self::IdxCol(target_col),
            _ => bail!("Expected CREATE statement but got:\n{}", create_sql),
        })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        match self {
            Self::TblCols { name_to_pos, .. } => IterEither::left(name_to_pos.keys().copied()),
            Self::IdxCol(col) => IterEither::right(once(*col)),
        }
    }

    pub fn has(&self, col: &str) -> bool {
        match self {
            Self::TblCols { name_to_pos, .. } => name_to_pos.contains_key(col),
            Self::IdxCol(c) => col == *c,
        }
    }

    pub fn is_int_pk(&self, col: &str) -> bool {
        matches!(
            self,
            Self::TblCols {
                int_pk: Some(c),
                ..
            } if *c == col
        )
    }

    pub fn record_pos(&self, col: &str) -> usize {
        match self {
            Self::TblCols { name_to_pos, .. } => name_to_pos[col],
            Self::IdxCol(_) => 0,
        }
    }
}
