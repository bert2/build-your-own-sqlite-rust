use crate::{
    format::{cell::*, record::*},
    schema::*,
    syntax::ast::*,
};
use anyhow::Result;
use std::convert::*;

#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    Null,
    Int(i64),
    Float(f64),
    Bytes(&'a [u8]),
    String(&'a str),
}

pub trait Eval<'a> {
    fn eval(&self, c: &LeafTblCell<'a>, s: &ObjSchema<'a>) -> Result<Value<'a>>;
}

impl<'a> Eval<'a> for Expr<'a> {
    fn eval(&self, cell: &LeafTblCell<'a>, schema: &ObjSchema<'a>) -> Result<Value<'a>> {
        Ok(match self {
            Expr::Null => Value::Null,
            Expr::String(s) => Value::String(s),
            Expr::Int(i) => Value::Int(*i),
            Expr::ColName(col) => {
                if schema.cols().is_int_pk(col) {
                    Value::Int(cell.row_id)
                } else {
                    Value::try_from(&cell.payload[schema.cols().record_pos(col)])?
                }
            }
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
            ColContent::Null => Value::Null,
            ColContent::Zero => Value::Int(0),
            ColContent::One => Value::Int(1),
            ColContent::Int8(_)
            | ColContent::Int16(_)
            | ColContent::Int24(_)
            | ColContent::Int32(_)
            | ColContent::Int48(_)
            | ColContent::Int64(_) => Value::Int(i64::try_from(content)?),
            ColContent::Float64(_) => Value::Float(f64::try_from(content)?),
            ColContent::Blob(bs) => Value::Bytes(bs),
            ColContent::Text(_) => Value::String(<&str>::try_from(content)?),
        })
    }
}
