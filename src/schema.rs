use crate::{cell::*, db_header::*, page_header::*};
use anyhow::*;
use std::convert::*;

#[derive(Debug)]
pub struct DbSchema<'a> {
    pub db_header: DbHeader,
    pub objs: Vec<Schema<'a>>,
    pub size: usize,
}

#[derive(Debug)]
pub struct Schema<'a> {
    pub type_: &'a str,
    pub name: &'a str,
    pub tbl_name: &'a str,
    pub rootpage: i64,
    pub sql: Option<&'a str>,
}

impl<'a> DbSchema<'a> {
    pub fn parse(db: &[u8]) -> Result<DbSchema> {
        let db_header = DbHeader::parse(&db[..DbHeader::SIZE])?;
        let page_header = PageHeader::parse(&db[DbHeader::SIZE..])?;
        let objs = db[DbHeader::SIZE + page_header.size()..]
            .chunks_exact(2)
            .take(page_header.number_of_cells.into())
            .map(|bytes| usize::from(u16::from_be_bytes(bytes.try_into().unwrap())))
            .map(|cell_pointer| Cell::parse(&db[cell_pointer..]).and_then(Schema::parse))
            .collect::<Result<Vec<_>>>()?;
        let size =
            usize::from(db_header.page_size - page_header.start_of_content_area) - DbHeader::SIZE;

        Ok(DbSchema {
            db_header,
            objs,
            size,
        })
    }

    pub fn table(&self, name: &str) -> Option<&Schema<'a>> {
        self.tables().find(|t| t.name == name)
    }

    pub fn tables(&self) -> impl Iterator<Item = &Schema<'a>> {
        self.objs.iter().filter(Schema::is_table)
    }

    pub fn indexes(&self) -> impl Iterator<Item = &Schema<'a>> {
        self.objs.iter().filter(Schema::is_index)
    }

    pub fn views(&self) -> impl Iterator<Item = &Schema<'a>> {
        self.objs.iter().filter(Schema::is_view)
    }

    pub fn triggers(&self) -> impl Iterator<Item = &Schema<'a>> {
        self.objs.iter().filter(Schema::is_trigger)
    }
}

impl<'a> Schema<'a> {
    pub fn parse(record: Cell<'a>) -> Result<Self> {
        Ok(Schema {
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

    pub fn offset(&self, page_size: usize) -> usize {
        debug_assert!(self.rootpage > 0);
        (self.rootpage - 1) as usize * page_size
    }

    pub fn is_table(self: &&Schema<'a>) -> bool {
        self.type_ == "table"
    }

    pub fn is_index(self: &&Schema<'a>) -> bool {
        self.type_ == "index"
    }

    pub fn is_view(self: &&Schema<'a>) -> bool {
        self.type_ == "view"
    }

    pub fn is_trigger(self: &&Schema<'a>) -> bool {
        self.type_ == "trigger"
    }
}
