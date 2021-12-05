use crate::cell::*;
use anyhow::Result;
use std::convert::TryFrom;

#[derive(Debug)]
pub struct Schema<'a> {
    pub type_: &'a str,
    pub name: &'a str,
    pub tbl_name: &'a str,
    pub rootpage: i64,
    pub sql: &'a str,
}

impl<'a> Schema<'a> {
    pub fn parse(record: Cell<'a>) -> Result<Self> {
        Ok(Self {
            type_: <&str>::try_from(&record.payload[0])?,
            name: <&str>::try_from(&record.payload[1])?,
            tbl_name: <&str>::try_from(&record.payload[2])?,
            rootpage: i64::try_from(&record.payload[3])?,
            sql: <&str>::try_from(&record.payload[4])?,
        })
    }
}
