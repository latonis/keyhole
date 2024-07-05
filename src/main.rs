use nom::bytes::complete::take;
use nom::IResult;
use std::fs::File;
use std::io::prelude::*;
use std::io::{self};

fn parse_magic(input: &[u8]) -> IResult<&[u8], &[u8]> {
    take(5usize)(input)
}

fn parse_version(input: &[u8]) -> IResult<&[u8], &[u8]> {
    take(4usize)(input)
}

fn parse_auxiliary_fields(input: &[u8]) -> IResult<&[u8], u8> {
    nom::number::complete::u8(input)
}

fn parse_four_byte_fields(input: &[u8]) -> IResult<&[u8], u64> {
    nom::number::complete::u64(nom::number::Endianness::Big)(input)
}

fn parse_rstring(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let (mut remainder, length) = nom::number::complete::u8(input)?;
    dbg!(length);

    dbg!(length & 0b11000000 as u8);
    let string_length: u64 = match length & 0b01000000 {
        00 => {
            let length = length & 0b00111111;
            dbg!(length);
            length as u64
        }
        0x40 => {
            // 01	Read one additional byte. The combined 14 bits represent the length
            todo!();
        }
        0x80 => {
            // 10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
            let size;
            (remainder, size) = parse_four_byte_fields(input).unwrap();
            dbg!(size);
            size
        }
        0xC0 => {
            // 11	The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
            todo!();
        }
        _ => unimplemented!(),
    };

    take(string_length as usize)(remainder)
}

#[derive(Default)]
struct RedisString {
    length: u32,
    value: String,
}

fn main() -> io::Result<()> {
    let mut f = File::open("dump.rdb")?;
    let mut buffer = vec![];
    f.read_to_end(&mut buffer)?;

    let (remaining, first_five) = parse_magic(&buffer[..]).unwrap();
    println!("Magic Bytes: {:X?}", first_five);

    let (remaining, version) = parse_version(remaining).unwrap();
    let version_val = std::str::from_utf8(&version)
        .unwrap()
        .parse::<u16>()
        .unwrap();
    println!("Version: {version_val} ({:X?})", version);
    let (remaining, opcode) = parse_auxiliary_fields(remaining).unwrap();
    println!("Aux Field: {:X?}", opcode);
    let mut remaining_parse = remaining;
    loop {
        match opcode {
            0xFA => {
                let (remaining, s1) = parse_rstring(remaining_parse).unwrap();
                dbg!(std::str::from_utf8(s1).unwrap());
                let (remaining, s2) = parse_rstring(remaining).unwrap();
                dbg!(std::str::from_utf8(s2).unwrap());
                remaining_parse = remaining;
            }
            _ => {
                println!("Aux Field: {:X?}", opcode);
                todo!()
            }
        }
    }

    Ok(())
}
