use std::fs::File;
use std::io::prelude::*;
use std::io::{self};

fn main() -> io::Result<()> {
    let mut f = File::open("testfiles/dump.rdb")?;
    let mut buffer = vec![];
    f.read_to_end(&mut buffer)?;

    let rdb_file = keyhole::parser::RDB::parse(&buffer);
    dbg!(rdb_file);

    Ok(())
}
