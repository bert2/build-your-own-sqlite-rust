#[derive(Debug)]
pub struct Schema {
    pub type_: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u8,
    pub sql: String,
}

impl Schema {
    /// Parses a record into a schema
    pub fn parse(record: Vec<Vec<u8>>) -> Option<Self> {
        let mut items = record.into_iter();
        let type_ = items.next()?;
        let name = items.next()?;
        let tbl_name = items.next()?;
        let rootpage = *items.next()?.get(0)?;
        let sql = items.next()?;

        let schema = Self {
            type_: String::from_utf8_lossy(&type_).to_string(),
            name: String::from_utf8_lossy(&name).to_string(),
            tbl_name: String::from_utf8_lossy(&tbl_name).to_string(),
            rootpage,
            sql: String::from_utf8_lossy(&sql).to_string(),
        };
        Some(schema)
    }
}
