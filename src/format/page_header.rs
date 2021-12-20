use anyhow::{bail, Result};
use std::convert::TryInto;

#[derive(Debug, PartialEq)]
pub enum PageType {
    IntrIdx = 2,
    IntrTbl = 5,
    LeafIdx = 10,
    LeafTbl = 13,
}

#[derive(Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub first_free_block_start: u16,
    pub number_of_cells: u16,
    pub start_of_content_area: u16,
    pub fragmented_free_bytes: u8,
    pub right_most_pointer: Option<u32>,
}

impl PageHeader {
    pub fn parse(stream: &[u8]) -> Result<Self> {
        let page_type = match stream[0] {
            2 => PageType::IntrIdx,
            5 => PageType::IntrTbl,
            10 => PageType::LeafIdx,
            13 => PageType::LeafTbl,
            x => bail!("Invalid page type encountered: {}", x),
        };
        let first_free_block_start = u16::from_be_bytes(stream[1..3].try_into()?);
        let number_of_cells = u16::from_be_bytes(stream[3..5].try_into()?);
        let start_of_content_area = u16::from_be_bytes(stream[5..7].try_into()?);
        let fragmented_free_bytes = stream[7];
        let right_most_pointer = match page_type {
            PageType::IntrTbl | PageType::IntrIdx => {
                Some(u32::from_be_bytes(stream[8..12].try_into()?))
            }
            _ => None,
        };

        Ok(Self {
            page_type,
            first_free_block_start,
            number_of_cells,
            start_of_content_area,
            fragmented_free_bytes,
            right_most_pointer,
        })
    }

    pub const fn is_leaf(&self) -> bool {
        matches!(self.page_type, PageType::LeafTbl | PageType::LeafIdx)
    }

    pub fn size(&self) -> usize {
        if self.is_leaf() {
            8
        } else {
            12
        }
    }
}
