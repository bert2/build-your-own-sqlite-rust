use crate::{
    format::{cell::*, db_header::*, page_header::*},
    util::*,
};
use anyhow::*;
use std::{
    convert::{TryFrom, TryInto},
    iter::{self, once},
};

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

    pub fn leaf_pages(
        self,
        page_size: usize,
        db: &'a [u8],
    ) -> impl Iterator<Item = Result<Page<'a>>> {
        if self.header.page_type == PageType::LeafTbl {
            return IterEither::left(iter::once(Ok(self)));
        }

        assert!(
            self.header.page_type == PageType::IntrTbl,
            "Cannot get leaf pages of {:?}",
            self.header.page_type
        );

        let cell_ptrs_offset =
            self.header.size() + if self.is_db_schema { DbHeader::SIZE } else { 0 };

        let right_most_child_page = self.header.right_most_pointer.unwrap();

        let leaves = self.data[cell_ptrs_offset..]
            .chunks_exact(2)
            .take(self.header.number_of_cells.into())
            .map(|bytes| usize::from(u16::from_be_bytes(bytes.try_into().unwrap())))
            .map(move |cell_ptr| IntrTblCell::parse(&self.data[cell_ptr..]))
            .map_ok_and_then(move |cell| Page::parse(cell.child_page, page_size, db))
            .chain(once(Page::parse(right_most_child_page, page_size, db)))
            .flat_map_ok_and_then(move |page| {
                Box::new(page.leaf_pages(page_size, db))
                    as Box<dyn Iterator<Item = Result<Page<'a>>>>
            });

        IterEither::right(leaves)
    }

    pub fn cells(self) -> impl Iterator<Item = Result<LeafTblCell<'a>>> {
        assert!(
            self.header.page_type == PageType::LeafTbl,
            "Cannot get cells of {:?}",
            self.header.page_type
        );

        let cell_ptrs_offset =
            self.header.size() + if self.is_db_schema { DbHeader::SIZE } else { 0 };

        self.data[cell_ptrs_offset..]
            .chunks_exact(2)
            .take(self.header.number_of_cells.into())
            .map(|bytes| usize::from(u16::from_be_bytes(bytes.try_into().unwrap())))
            .map(move |cell_ptr| LeafTblCell::parse(&self.data[cell_ptr..]))
    }
}
