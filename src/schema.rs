use crate::cell::*;
use anyhow::*;
use std::convert::*;

#[derive(Debug)]
pub struct Schema<'a> {
    pub type_: &'a str,
    pub name: &'a str,
    pub tbl_name: &'a str,
    pub rootpage: i64,
    pub sql: Option<&'a str>,
}

impl<'a> Schema<'a> {
    pub fn parse(record: Cell<'a>) -> Result<Self> {
        Ok(Self {
            type_: <&str>::try_from(&record.payload[0])
                .map_err(|e| anyhow!("Unexpected value in column 'type': {}", e))?,
            name: <&str>::try_from(&record.payload[1])
                .map_err(|e| anyhow!("Unexpected value in column 'name': {}", e))?,
            tbl_name: <&str>::try_from(&record.payload[2])
                .map_err(|e| anyhow!("Unexpected value in column 'tbl_name': {}", e))?,
            rootpage: i64::try_from(&record.payload[3])
                .map_err(|e| anyhow!("Unexpected value in column 'rootpage': {}", e))?,
            sql: Option::<&str>::try_from(&record.payload[4])
                .map_err(|e| anyhow!("Unexpected value in column 'sql': {}", e))?,
        })
    }
}
