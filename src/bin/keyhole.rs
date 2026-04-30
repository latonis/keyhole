use std::fs::File;
use std::io::prelude::*;
use std::io::{self};

fn main() -> io::Result<()> {
    let mut f = File::open("tests/dump.rdb")?;
    let mut buffer = vec![];
    f.read_to_end(&mut buffer)?;

    let rdb = keyhole::RDB::new(&buffer)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    dbg!(rdb);

    Ok(())
}
