use anyhow::{bail, Result};
use std::convert::TryInto;

#[derive(Debug)]
pub enum Enc {
    Utf8 = 1,
    Utf16Le = 2,
    Utf16Be = 3,
}

#[derive(Debug)]
pub struct DbHeader {
    pub header_string: String,
    pub page_size: u16,
    pub write_format: u8,
    pub read_format: u8,
    pub reserved_bytes: u8,
    pub max_emb_payload_frac: u8,
    pub min_emb_payload_frac: u8,
    pub leaf_payload_frac: u8,
    pub file_change_counter: u32,
    pub db_page_count: u32,
    pub first_freelist_page: u32,
    pub freelist_page_count: u32,
    pub schema_cookie: u32,
    pub schema_format: u32,
    pub default_cache_size: u32,
    pub autovacuum_top_root: u32,
    pub text_encoding: Enc,
    pub user_version: u32,
    pub incremental_vacuum: u32,
    pub application_id: u32,
    pub reserved: [u8; 20],
    pub version_valid_for: u32,
    pub software_version: u32,
}

impl DbHeader {
    pub fn parse(stream: &[u8]) -> Result<Self> {
        Ok(DbHeader {
            header_string: String::from_utf8_lossy(&stream[..16]).to_string(),
            page_size: u16::from_be_bytes(stream[16..18].try_into()?),
            write_format: stream[18],
            read_format: stream[19],
            reserved_bytes: stream[20],
            max_emb_payload_frac: stream[21],
            min_emb_payload_frac: stream[22],
            leaf_payload_frac: stream[23],
            file_change_counter: u32::from_be_bytes(stream[24..28].try_into()?),
            db_page_count: u32::from_be_bytes(stream[28..32].try_into()?),
            first_freelist_page: u32::from_be_bytes(stream[32..36].try_into()?),
            freelist_page_count: u32::from_be_bytes(stream[36..40].try_into()?),
            schema_cookie: u32::from_be_bytes(stream[40..44].try_into()?),
            schema_format: u32::from_be_bytes(stream[44..48].try_into()?),
            default_cache_size: u32::from_be_bytes(stream[48..52].try_into()?),
            autovacuum_top_root: u32::from_be_bytes(stream[52..56].try_into()?),
            text_encoding: match u32::from_be_bytes(stream[56..60].try_into()?) {
                1 => Enc::Utf8,
                2 => Enc::Utf16Le,
                3 => Enc::Utf16Be,
                x => bail!("Invalid text encoding value encountered: {}", x),
            },
            user_version: u32::from_be_bytes(stream[60..64].try_into()?),
            incremental_vacuum: u32::from_be_bytes(stream[64..68].try_into()?),
            application_id: u32::from_be_bytes(stream[68..72].try_into()?),
            reserved: stream[72..92].try_into()?,
            version_valid_for: u32::from_be_bytes(stream[92..96].try_into()?),
            software_version: u32::from_be_bytes(stream[96..100].try_into()?),
        })
    }
}
