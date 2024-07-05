use nom::bytes::complete::take;
use nom::IResult;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self};


#[derive(Default)]
struct RedisString {
    length: u32,
    value: String,
}

fn main() -> io::Result<()> {
    let mut f = File::open("testfiles/dump.rdb")?;
    let mut buffer = vec![];
    f.read_to_end(&mut buffer)?;

    let rdb_file = rdb::parser::RDB::parse(&buffer);
    dbg!(rdb_file);

    Ok(())
}
