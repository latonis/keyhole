use std::fs::File;
use std::io::prelude::*;
use std::io::{self};

fn main() -> io::Result<()> {
    let mut f = File::open("tests/dump.rdb")?;
    let mut buffer = vec![];
    f.read_to_end(&mut buffer)?;

    let rdb = keyhole::parser::RDB::new(&buffer);
    
    dbg!(rdb);

    Ok(())
}
