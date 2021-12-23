use crate::{
    format::{IntrIdxCell, IntrTblCell, LeafIdxCell, LeafTblCell, Page, PageType},
    interpreter::eval::Value,
    util::{FlatMapOkAndThenExt, IterEither, MapOkAndThenExt},
};
use anyhow::Result;
use itertools::Itertools;
use std::{convert::TryFrom, iter::once};

pub fn full_tbl_scan<'a>(
    page: Page<'a>,
    page_size: usize,
    db: &'a [u8],
) -> impl Iterator<Item = Result<LeafTblCell<'a>>> {
    fn leaf_pages<'a>(
        page: Page<'a>,
        page_size: usize,
        db: &'a [u8],
    ) -> impl Iterator<Item = Result<Page<'a>>> {
        if page.header.page_type == PageType::LeafTbl {
            return IterEither::left(once(Ok(page)));
        }

        assert!(
            page.header.page_type == PageType::IntrTbl,
            "Cannot get leaf pages of {:?}",
            page.header.page_type
        );

        let right_most_child_page = page.header.right_most_ptr.unwrap_or_else(|| {
            panic!(
                "Expected {:?} to have right most child page pointer",
                page.header.page_type
            )
        });

        let leaves = page
            .cell_ptrs()
            .map(move |ptr| IntrTblCell::parse(&page.data[ptr..]))
            .map_ok_and_then(move |cell| Page::parse(cell.child_page, page_size, db))
            .chain(once(Page::parse(right_most_child_page, page_size, db)))
            .flat_map_ok_and_then(move |p| {
                Box::new(leaf_pages(p, page_size, db)) as Box<dyn Iterator<Item = Result<Page<'a>>>>
            });

        IterEither::right(leaves)
    }

    leaf_pages(page, page_size, db).flat_map_ok_and_then(|p| {
        p.cell_ptrs()
            .map(move |ptr| LeafTblCell::parse(&p.data[ptr..]))
    })
}

pub fn pk_scan<'a>(
    pk: i64,
    page: &Page<'a>,
    page_size: usize,
    db: &'a [u8],
) -> Result<Option<LeafTblCell<'a>>> {
    if page.header.page_type == PageType::LeafTbl {
        for cell in page
            .cell_ptrs()
            .map(move |ptr| LeafTblCell::parse(&page.data[ptr..]))
        {
            let cell = cell?;
            if cell.row_id == pk {
                return Ok(Some(cell));
            }
        }

        return Ok(None);
    }

    assert!(
        page.header.page_type == PageType::IntrTbl,
        "Cannot search cells by integer primary key in {:?}",
        page.header.page_type
    );

    let intr_cells = page
        .cell_ptrs()
        .map(|ptr| IntrTblCell::parse(&page.data[ptr..]));

    for cell in intr_cells {
        let cell = cell?;
        if pk <= cell.row_id {
            return pk_scan(
                pk,
                &Page::parse(cell.child_page, page_size, db)?,
                page_size,
                db,
            );
        }
    }

    let right_most_child_page = page
        .header
        .right_most_ptr
        .map(|ptr| Page::parse(ptr, page_size, db))
        .transpose()?
        .unwrap_or_else(|| {
            panic!(
                "Expected {:?} to have right most child page pointer",
                page.header.page_type
            )
        });

    pk_scan(pk, &right_most_child_page, page_size, db)
}

pub fn idx_scan<'a>(
    key: Value<'a>,
    idx_page: Page<'a>,
    tbl_page: &'a Page,
    page_size: usize,
    db: &'a [u8],
) -> impl Iterator<Item = Result<LeafTblCell<'a>>> {
    fn find_idx_cells<'a>(
        key: Value<'a>,
        idx_page: Page<'a>,
        page_size: usize,
        db: &'a [u8],
    ) -> impl Iterator<Item = Result<LeafIdxCell<'a>>> {
        if idx_page.header.page_type == PageType::LeafIdx {
            let cells = idx_page
                .cell_ptrs()
                .map(move |ptr| LeafIdxCell::parse(&idx_page.data[ptr..]).unwrap())
                .skip_while(move |cell| Value::try_from(&cell.payload[0]).unwrap() < key)
                .take_while(move |cell| Value::try_from(&cell.payload[0]).unwrap() == key)
                .map(Ok);

            return IterEither::left(cells);
        }
        assert!(
            idx_page.header.page_type == PageType::IntrIdx,
            "Cannot search cells by index in {:?}",
            idx_page.header.page_type
        );

        let right_most_child_page = idx_page.header.right_most_ptr.unwrap_or_else(|| {
            panic!(
                "Expected {:?} to have right most child page pointer",
                idx_page.header.page_type
            )
        });

        let cells = idx_page
            .cell_ptrs()
            .map(|ptr| IntrIdxCell::parse(&idx_page.data[ptr..]).unwrap())
            .find(|cell| key <= Value::try_from(&cell.payload[0]).unwrap())
            .into_iter()
            .map(move |cell| Page::parse(cell.child_page, page_size, db))
            .chain(once(Page::parse(right_most_child_page, page_size, db)))
            .flat_map_ok_and_then(move |page| {
                Box::new(find_idx_cells(key, page, page_size, db))
                    as Box<dyn Iterator<Item = Result<LeafIdxCell<'a>>>>
            });

        IterEither::right(cells)
    }

    find_idx_cells(key, idx_page, page_size, db)
        .map_ok_and_then(|cell| i64::try_from(&cell.payload[1]))
        .map_ok_and_then(move |pk| pk_scan(pk, tbl_page, page_size, db))
        .flatten_ok()
}
