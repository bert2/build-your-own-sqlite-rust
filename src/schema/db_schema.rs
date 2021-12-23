use crate::{
    format::{DbHeader, Page},
    interpreter::btree,
    schema::ObjSchema,
    util::MapOkAndThenExt,
};
use anyhow::Result;

#[derive(Debug)]
pub struct DbSchema<'a> {
    pub db_header: DbHeader,
    pub objs: Vec<ObjSchema<'a>>,
    pub size: usize,
}

impl<'a> DbSchema<'a> {
    pub fn parse(db: &'a [u8]) -> Result<DbSchema<'a>> {
        let db_header = DbHeader::parse(&db[..DbHeader::SIZE])?;
        let page_size = db_header.page_size.into();
        let rootpage = Page::parse_schema(page_size, db)?;
        let page_content_offset: usize = rootpage.header.start_of_content_area.into();

        Ok(DbSchema {
            db_header,
            objs: btree::full_tbl_scan(rootpage, page_size, db)
                .map_ok_and_then(|c| ObjSchema::parse(&c))
                .collect::<Result<Vec<_>>>()?,
            size: page_size - page_content_offset - DbHeader::SIZE,
        })
    }

    pub fn tables(&self) -> impl Iterator<Item = &ObjSchema<'a>> {
        self.objs.iter().filter(ObjSchema::is_table)
    }

    pub fn indexes(&self) -> impl Iterator<Item = &ObjSchema<'a>> {
        self.objs.iter().filter(ObjSchema::is_index)
    }

    pub fn views(&self) -> impl Iterator<Item = &ObjSchema<'a>> {
        self.objs.iter().filter(ObjSchema::is_view)
    }

    pub fn triggers(&self) -> impl Iterator<Item = &ObjSchema<'a>> {
        self.objs.iter().filter(ObjSchema::is_trigger)
    }

    pub fn table(&self, name: &str) -> Option<&ObjSchema<'a>> {
        self.tables().find(|t| t.name == name)
    }

    pub fn index(&self, tbl: &str, col: &str) -> Option<&ObjSchema<'a>> {
        self.indexes().find(|s| {
            !s.name.starts_with("sqlite_autoindex_") && s.tbl_name == tbl && s.cols().has(col)
        })
    }

    pub fn has_index(&self, tbl: &str, col: &str) -> bool {
        self.indexes()
            .any(|s| s.tbl_name == tbl && s.cols().has(col))
    }
}
