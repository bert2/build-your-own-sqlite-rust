use crate::{
    format::{ColContent, LeafTblCell},
    schema::ObjSchema,
    syntax::{BoolExpr, Expr, Literal},
};
use anyhow::Result;
use std::{convert::TryFrom, fmt};

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd)]
pub enum Value<'a> {
    Null,
    Int(i64),
    Float(f64),
    Bytes(&'a [u8]),
    String(&'a str),
    CountPlaceholder,
}

pub trait Eval<'a> {
    fn eval(&self, c: &LeafTblCell<'a>, s: &ObjSchema<'a>) -> Result<Value<'a>>;
}

impl<'a> Eval<'a> for Expr<'a> {
    fn eval(&self, cell: &LeafTblCell<'a>, schema: &ObjSchema<'a>) -> Result<Value<'a>> {
        Ok(match self {
            Expr::Literal(l) => l.into(),
            Expr::ColName(col) => {
                if schema.cols().is_int_pk(col) {
                    Value::Int(cell.row_id)
                } else {
                    Value::try_from(&cell.payload[schema.cols().record_pos(col)])?
                }
            }
            Expr::Count => Value::CountPlaceholder,
        })
    }
}

impl<'a> Eval<'a> for BoolExpr<'a> {
    fn eval(&self, c: &LeafTblCell<'a>, s: &ObjSchema<'a>) -> Result<Value<'a>> {
        Ok(match self {
            BoolExpr::Equals { l, r } => Value::Int((l.eval(c, s)? == r.eval(c, s)?) as i64),
            BoolExpr::NotEquals { l, r } => Value::Int((l.eval(c, s)? != r.eval(c, s)?) as i64),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for Value<'a> {
    type Error = anyhow::Error;

    fn try_from(content: &ColContent<'a>) -> Result<Self, Self::Error> {
        Ok(match content {
            ColContent::Null => Self::Null,
            ColContent::Zero => Self::Int(0),
            ColContent::One => Self::Int(1),
            ColContent::Int8(_)
            | ColContent::Int16(_)
            | ColContent::Int24(_)
            | ColContent::Int32(_)
            | ColContent::Int48(_)
            | ColContent::Int64(_) => Self::Int(i64::try_from(content)?),
            ColContent::Float64(_) => Self::Float(f64::try_from(content)?),
            ColContent::Blob(bs) => Self::Bytes(bs),
            ColContent::Text(_) => Self::String(<&str>::try_from(content)?),
        })
    }
}

impl<'a> From<&Literal<'a>> for Value<'a> {
    fn from(expr: &Literal<'a>) -> Self {
        match expr {
            Literal::Null => Self::Null,
            Literal::Int(n) => Self::Int(*n),
            Literal::String(s) => Self::String(s),
        }
    }
}

impl<'a> fmt::Display for Value<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(x) => write!(f, "{}", x),
            Value::Float(x) => write!(f, "{}", x),
            Value::Bytes(bytes) => {
                for byte in *bytes {
                    write!(f, "{:02X} ", byte)?;
                }
                Ok(())
            }
            Value::String(s) => write!(f, "{}", s),
            Value::CountPlaceholder => panic!("Unexpected CountPlaceholder"),
        }
    }
}
