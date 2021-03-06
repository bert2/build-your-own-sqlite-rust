use anyhow::{anyhow, bail, Result};
use std::{
    convert::{TryFrom, TryInto},
    str,
};

#[derive(Debug)]
pub enum ColContent<'a> {
    Null,
    Int8(&'a [u8; 1]),
    Int16(&'a [u8; 2]),
    Int24(&'a [u8; 3]),
    Int32(&'a [u8; 4]),
    Int48(&'a [u8; 6]),
    Int64(&'a [u8; 8]),
    Float64(&'a [u8; 8]),
    Zero,
    One,
    Blob(&'a [u8]),
    Text(&'a [u8]),
}

impl<'a> ColContent<'a> {
    pub fn parse(serial_type: i64, stream: &[u8]) -> Result<(ColContent, usize)> {
        Ok(match serial_type {
            0 => (ColContent::Null, 0),
            1 => (ColContent::Int8(stream[..1].try_into()?), 1),
            2 => (ColContent::Int16(stream[..2].try_into()?), 2),
            3 => (ColContent::Int24(stream[..3].try_into()?), 3),
            4 => (ColContent::Int32(stream[..4].try_into()?), 4),
            5 => (ColContent::Int48(stream[..6].try_into()?), 6),
            6 => (ColContent::Int64(stream[..8].try_into()?), 8),
            7 => (ColContent::Float64(stream[..8].try_into()?), 8),
            8 => (ColContent::Zero, 0),
            9 => (ColContent::One, 0),
            n if n >= 12 && n % 2 == 0 => {
                let len = ((n - 12) / 2).try_into()?;
                (ColContent::Blob(&stream[..len]), len)
            }
            n if n >= 13 && n % 2 == 1 => {
                let len = ((n - 13) / 2).try_into()?;
                (ColContent::Text(&stream[..len]), len)
            }
            n => bail!("Invalid serial type: {}", n),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for i8 {
    type Error = anyhow::Error;

    fn try_from(value: &ColContent) -> Result<Self, Self::Error> {
        Ok(match value {
            ColContent::Zero => 0,
            ColContent::One => 1,
            ColContent::Int8(&bytes) => Self::from_be_bytes(bytes),
            _ => bail!("ColContent cannot be converted to i8: {:?}", value),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for i16 {
    type Error = anyhow::Error;

    fn try_from(value: &ColContent) -> Result<Self, Self::Error> {
        Ok(match value {
            ColContent::Int16(&bytes) => Self::from_be_bytes(bytes),
            _ => i8::try_from(value)
                .map_err(|_| anyhow!("ColContent cannot be converted to i16: {:?}", value))?
                .into(),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for i32 {
    type Error = anyhow::Error;

    fn try_from(value: &ColContent) -> Result<Self, Self::Error> {
        Ok(match value {
            ColContent::Int32(&bytes) => Self::from_be_bytes(bytes),
            ColContent::Int24(&bytes) => i32_from_3_be_bytes(bytes),
            _ => i16::try_from(value)
                .map_err(|_| anyhow!("ColContent cannot be converted to i32: {:?}", value))?
                .into(),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for i64 {
    type Error = anyhow::Error;

    fn try_from(value: &ColContent) -> Result<Self, Self::Error> {
        Ok(match value {
            ColContent::Int64(&bytes) => Self::from_be_bytes(bytes),
            ColContent::Int48(&bytes) => i64_from_6_be_bytes(bytes),
            _ => i32::try_from(value)
                .map_err(|_| anyhow!("ColContent cannot be converted to i64: {:?}", value))?
                .into(),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for f64 {
    type Error = anyhow::Error;

    fn try_from(value: &ColContent) -> Result<Self, Self::Error> {
        Ok(match value {
            ColContent::Float64(&bytes) => Self::from_be_bytes(bytes),
            _ => bail!("ColContent cannot be converted to f64: {:?}", value),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for &'a str {
    type Error = anyhow::Error;

    fn try_from(value: &ColContent<'a>) -> Result<Self, Self::Error> {
        Ok(match value {
            ColContent::Text(bytes) => str::from_utf8(bytes)?,
            _ => bail!("ColContent cannot be converted to str: {:?}", value),
        })
    }
}

impl<'a> TryFrom<&ColContent<'a>> for Option<&'a str> {
    type Error = anyhow::Error;

    fn try_from(value: &ColContent<'a>) -> Result<Self, Self::Error> {
        Ok(match value {
            ColContent::Null => None,
            _ => Some(<&str>::try_from(value)?),
        })
    }
}

fn i32_from_3_be_bytes(bytes: [u8; 3]) -> i32 {
    (i32::from(bytes[0]) << 16) | (i32::from(bytes[1]) << 8) | i32::from(bytes[2])
}

fn i64_from_6_be_bytes(bytes: [u8; 6]) -> i64 {
    (i64::from(bytes[0]) << 40)
        | (i64::from(bytes[1]) << 32)
        | (i64::from(bytes[2]) << 24)
        | (i64::from(bytes[3]) << 16)
        | (i64::from(bytes[4]) << 8)
        | i64::from(bytes[5])
}
