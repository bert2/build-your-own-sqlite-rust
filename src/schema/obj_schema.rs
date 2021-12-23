use crate::{format::LeafTblCell, schema::Cols};
use anyhow::{anyhow, Result};
use std::convert::TryFrom;

#[derive(Debug)]
pub struct ObjSchema<'a> {
    pub type_: &'a str,
    pub name: &'a str,
    pub tbl_name: &'a str,
    pub rootpage: i32,
    pub sql: Option<&'a str>,
    pub cols: Option<Cols<'a>>,
}

impl<'a> ObjSchema<'a> {
    pub fn parse(record: &LeafTblCell<'a>) -> Result<Self> {
        let type_ = <&str>::try_from(&record.payload[0])
            .map_err(|e| anyhow!("Unexpected value in column 'type': {}", e))?;
        let name = <&str>::try_from(&record.payload[1])
            .map_err(|e| anyhow!("Unexpected value in column 'name': {}", e))?;
        let tbl_name = <&str>::try_from(&record.payload[2])
            .map_err(|e| anyhow!("Unexpected value in column 'tbl_name': {}", e))?;
        let rootpage = i32::try_from(&record.payload[3])
            .map_err(|e| anyhow!("Unexpected value in column 'rootpage': {}", e))?;
        let sql = Option::<&str>::try_from(&record.payload[4])
            .map_err(|e| anyhow!("Unexpected value in column 'sql': {}", e))?;
        let cols = sql.map(Cols::parse).transpose()?;

        Ok(Self {
            type_,
            name,
            tbl_name,
            rootpage,
            sql,
            cols,
        })
    }

    pub fn cols(&self) -> &Cols {
        self.cols
            .as_ref()
            .unwrap_or_else(|| panic!(
                "Columns of object '{}' are unknown, because no CREATE statement was found in schema record",
                self.name))
    }

    pub fn is_table(self: &&ObjSchema<'a>) -> bool {
        self.type_ == "table"
    }

    pub fn is_index(self: &&ObjSchema<'a>) -> bool {
        self.type_ == "index"
    }

    pub fn is_view(self: &&ObjSchema<'a>) -> bool {
        self.type_ == "view"
    }

    pub fn is_trigger(self: &&ObjSchema<'a>) -> bool {
        self.type_ == "trigger"
    }

    pub fn is_sequence_tbl(self: &ObjSchema<'a>) -> bool {
        self.name == "sqlite_sequence"
    }
}
