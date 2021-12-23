use crate::format::{varint, ColContent};
use anyhow::Result;
use std::{convert::TryInto, ops::Index};

#[derive(Debug)]
pub struct Record<'a>(pub Vec<ColContent<'a>>);

impl<'a> Record<'a> {
    pub fn parse(stream: &'a [u8]) -> Result<Self> {
        let (header_size, mut header_offset) = varint::parse(stream);
        let header_size = header_size.try_into()?;
        let mut content_offset = header_size;

        let mut record = vec![];

        while header_offset < header_size {
            let (serial_type, read_bytes) = varint::parse(&stream[header_offset..]);
            let (col_content, content_size) =
                ColContent::parse(serial_type, &stream[content_offset..])?;
            record.push(col_content);
            header_offset += read_bytes;
            content_offset += content_size;
        }

        Ok(Record(record))
    }
}

impl<'a> Index<usize> for Record<'a> {
    type Output = ColContent<'a>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}
