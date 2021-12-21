use crate::format::{record::Record, varint};
use anyhow::Result;
use std::convert::TryInto;

#[derive(Debug)]
pub struct LeafTblCell<'a> {
    pub payload_size: usize,
    pub row_id: i64,
    pub payload: Record<'a>,
}

impl<'a> LeafTblCell<'a> {
    pub fn parse(stream: &'a [u8]) -> Result<Self> {
        let mut offset = 0;

        let (payload_size, bytes_read) = varint::parse(stream);
        let payload_size = payload_size.try_into()?;
        offset += bytes_read;

        let (row_id, bytes_read) = varint::parse(&stream[offset..]);
        offset += bytes_read;

        Ok(Self {
            payload_size,
            row_id,
            payload: Record::parse(&stream[offset..offset + payload_size])?,
        })
    }
}

#[derive(Debug)]
pub struct IntrTblCell {
    pub child_page: i32,
    pub row_id: i64,
}

impl<'a> IntrTblCell {
    pub fn parse(stream: &'a [u8]) -> Result<Self> {
        let child_page = i32::from_be_bytes(stream[..4].try_into()?);
        let (row_id, _) = varint::parse(&stream[4..]);
        Ok(Self { child_page, row_id })
    }
}
