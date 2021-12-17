use crate::{
    format::{cell::*, db_header::*, page_header::*},
    util::BindMapOkExt,
};
use anyhow::*;
use itertools::Itertools;
use std::convert::{TryFrom, TryInto};

#[derive(Debug)]
pub struct Page<'a> {
    pub header: PageHeader,
    pub data: &'a [u8],
    pub is_db_schema: bool,
}

impl<'a> Page<'a> {
    pub fn parse_schema(page_size: usize, db: &'a [u8]) -> Result<Self> {
        Ok(Page {
            header: PageHeader::parse(&db[DbHeader::SIZE..])?,
            data: &db[..page_size],
            is_db_schema: true,
        })
    }

    pub fn parse(rootpage: i32, page_size: usize, db: &'a [u8]) -> Result<Self> {
        let page_offset = usize::try_from(rootpage - 1).unwrap() * page_size;

        Ok(Page {
            header: PageHeader::parse(&db[page_offset..])?,
            data: &db[page_offset..page_offset + page_size],
            is_db_schema: false,
        })
    }

    pub fn leaf_pages(self, page_size: usize, db: &'a [u8]) -> Result<Vec<Page<'a>>> {
        if self.header.page_type == PageType::LeafTable {
            return Ok(vec![self]);
        }

        let cell_ptrs_offset =
            self.header.size() + if self.is_db_schema { DbHeader::SIZE } else { 0 };

        let pages = self.data[cell_ptrs_offset..]
            .chunks_exact(2)
            .take(self.header.number_of_cells.into())
            .map(|bytes| usize::from(u16::from_be_bytes(bytes.try_into().unwrap())))
            .map(|cell_pointer| IntrTblCell::parse(&self.data[cell_pointer..]))
            .bind_map_ok(|cell| Page::parse(cell.child_page, page_size, db))
            .bind_map_ok(|page| page.leaf_pages(page_size, db))
            .flatten_ok()
            .collect::<Result<_>>()?;

        Ok(pages)
    }

    pub fn cells(self) -> Result<Vec<LeafTblCell<'a>>> {
        if self.header.page_type != PageType::LeafTable {
            bail!("Cannot get cells of {:?}", self.header.page_type);
        }

        let cell_ptrs_offset =
            self.header.size() + if self.is_db_schema { DbHeader::SIZE } else { 0 };

        self.data[cell_ptrs_offset..]
            .chunks_exact(2)
            .take(self.header.number_of_cells.into())
            .map(|bytes| usize::from(u16::from_be_bytes(bytes.try_into().unwrap())))
            .map(|cell_pointer| LeafTblCell::parse(&self.data[cell_pointer..]))
            .collect::<Result<_>>()
    }
}
