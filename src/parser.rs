type Error<'a> = nom::error::Error<&'a [u8]>;
use nom::bytes::complete::take;
use nom::IResult;

#[derive(Default, Debug)]
pub struct RDB<'a> {
    magic: &'a [u8],
    version: u32,
    auxiliary_commands: Vec<AuxiliaryCommand>,
}

#[derive(Default, Debug)]
struct AuxiliaryCommand {
    opcode: u8,
}

#[derive(Default)]
struct RedisString {
    length: u32,
    value: String,
}

impl<'a> RDB<'a> {
    pub fn parse(data: &'a [u8]) -> Result<Self, nom::error::Error<nom::error::ErrorKind>> {
        let (remaining, magic) = RDB::parse_magic(data).unwrap();

        let (remaining, version) = RDB::parse_version(remaining).unwrap();
        let version = std::str::from_utf8(&version)
            .unwrap()
            .parse::<u32>()
            .unwrap();

        // println!("Aux Field: {:X?}", opcode);
        let mut remaining_parse = remaining;
        let mut aux_commands = Vec::<AuxiliaryCommand>::new();
        loop {
            let (remaining, opcode) = RDB::parse_auxiliary_field(remaining).unwrap();
            match opcode {
                0xFA => {
                    let (remaining, s1) = RDB::parse_rstring(remaining_parse).unwrap();
                    dbg!(std::str::from_utf8(s1).unwrap());
                    let (remaining, s2) = RDB::parse_rstring(remaining).unwrap();
                    dbg!(std::str::from_utf8(s2).unwrap());
                    remaining_parse = remaining;
                }
                _ => {
                    println!("Aux Field: {:X?}", opcode);
                    break;
                }
            }
            aux_commands.push(AuxiliaryCommand { opcode });
        }

        Ok(RDB {
            magic,
            version,
            auxiliary_commands: aux_commands,
            ..Default::default()
        })
    }

    fn parse_magic(input: &'a [u8]) -> IResult<&[u8], &[u8]> {
        take(5usize)(input)
    }

    fn parse_version(input: &[u8]) -> IResult<&[u8], &[u8]> {
        take(4usize)(input)
    }

    fn parse_auxiliary_field(input: &[u8]) -> IResult<&[u8], u8> {
        nom::number::complete::u8(input)
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
                (remainder, size) =
                    nom::number::complete::u64(nom::number::Endianness::Big)(input)?;

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
}
