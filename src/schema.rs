use crate::{
    format::{cell::*, db_header::*, page::*},
    syntax::{ast::*, parse},
    util::*,
};
use anyhow::*;
use std::{collections::HashMap, convert::*};

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
    pub rootpage: i32,
    pub sql: Option<&'a str>,
    pub cols: Option<Cols<'a>>,
}

#[derive(Debug)]
pub struct Cols<'a> {
    pub int_pk: Option<&'a str>,
    pub name_to_idx: HashMap<&'a str, usize>,
}

impl<'a> DbSchema<'a> {
    pub fn parse(db: &'a [u8]) -> Result<DbSchema<'a>> {
        let db_header = DbHeader::parse(&db[..DbHeader::SIZE])?;
        let page_size = db_header.page_size.into();
        let rootpage = Page::parse_schema(page_size, db)?;
        let page_content_offset: usize = rootpage.header.start_of_content_area.into();

        Ok(DbSchema {
            db_header,
            objs: rootpage
                .leaf_pages(page_size, db)
                .flat_map_ok_and_then(Page::cells)
                .map_ok_and_then(Schema::parse)
                .collect::<Result<Vec<_>>>()?,
            size: page_size - page_content_offset - DbHeader::SIZE,
        })
    }

    pub fn table(self, name: &str) -> Option<Schema<'a>> {
        self.objs
            .into_iter()
            .filter(|s| s.is_table())
            .find(|t| t.name == name)
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
    pub fn parse(record: LeafTblCell<'a>) -> Result<Self> {
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

        Ok(Schema {
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

impl<'a> Cols<'a> {
    pub fn parse(tbl_sql: &'a str) -> Result<Cols<'a>> {
        let sql = parse::sql_stmt(tbl_sql)
            .map_err(|e| anyhow!("Failed to parse CREATE TABLE statement: {}", e))?;
        let col_defs = match sql {
            SqlStmt::CreateTbl { col_defs, .. } => col_defs,
            _ => bail!("Expected CREATE TABLE statement but got:\n{}", tbl_sql),
        };

        Ok(Cols {
            int_pk: col_defs.iter().find(ColDef::is_int_pk).map(ColDef::name),
            name_to_idx: col_defs
                .iter()
                .map(ColDef::name)
                .enumerate()
                .map(flip)
                .collect::<HashMap<_, _>>(),
        })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.name_to_idx.keys().copied()
    }

    pub fn has(&self, col: &str) -> bool {
        self.name_to_idx.contains_key(col)
    }

    pub fn is_int_pk(&self, col: &str) -> bool {
        self.int_pk.contains_(&col)
    }

    pub fn index(&self, col: &str) -> usize {
        self.name_to_idx[col]
    }
}
