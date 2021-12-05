use crate::{record::*, varint::parse_varint};
use anyhow::Result;
use std::convert::TryInto;

#[derive(Debug)]
pub struct Cell<'a> {
    pub payload_size: usize,
    pub row_id: i64,
    pub payload: Record<'a>,
}

impl<'a> Cell<'a> {
    pub fn parse(stream: &'a [u8]) -> Result<Self> {
        let mut offset = 0;

        let (payload_size, bytes_read) = parse_varint(&stream);
        let payload_size = payload_size.try_into()?;
        offset += bytes_read;

        let (row_id, bytes_read) = parse_varint(&stream[offset..]);
        offset += bytes_read;

        Ok(Cell {
            payload_size,
            row_id,
            payload: Record::parse(&stream[offset..])?,
        })
    }
}
