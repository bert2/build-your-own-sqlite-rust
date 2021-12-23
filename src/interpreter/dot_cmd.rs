use crate::{schema::DbSchema, syntax::DotCmd};
use anyhow::Result;

pub fn run(cmd: DotCmd, db_schema: &DbSchema) -> Result<()> {
    Ok(match cmd {
        DotCmd::DbInfo => dbinfo(db_schema),
        DotCmd::Tables => tables(db_schema),
        DotCmd::Schema => schema(db_schema),
    })
}

fn dbinfo(db_schema: &DbSchema) -> () {
    let s = db_schema;
    let h = &s.db_header;

    println!("database page size:  {}", h.page_size);
    println!("write format:        {}", h.write_format);
    println!("read format:         {}", h.read_format);
    println!("reserved bytes:      {}", h.reserved_bytes);
    println!("file change counter: {}", h.file_change_counter);
    println!("database page count: {}", h.db_page_count);
    println!("freelist page count: {}", h.freelist_page_count);
    println!("schema cookie:       {}", h.schema_cookie);
    println!("schema format:       {}", h.schema_format);
    println!("default cache size:  {}", h.default_cache_size);
    println!("autovacuum top root: {}", h.autovacuum_top_root);
    println!("incremental vacuum:  {}", h.incremental_vacuum);
    println!("text encoding:       {}", h.text_encoding);
    println!("user version:        {}", h.user_version);
    println!("application id:      {}", h.application_id);
    println!("software version:    {}", h.software_version);
    println!("number of tables:    {}", s.tables().count());
    println!("number of indexes:   {}", s.indexes().count());
    println!("number of triggers:  {}", s.triggers().count());
    println!("number of views:     {}", s.views().count());
    println!("schema size:         {}", s.size);
}

fn tables(db_schema: &DbSchema) -> () {
    db_schema
        .tables()
        .filter(|t| !t.is_sequence_tbl())
        .for_each(|t| print!("{} ", t.name));
}

fn schema(db_schema: &DbSchema) -> () {
    db_schema.objs.iter().for_each(|schema| match schema.sql {
        Some(sql) => println!("{};", sql),
        None => println!(
            "-- The {} '{}' has no CREATE statement",
            schema.type_, schema.name
        ),
    });
}
