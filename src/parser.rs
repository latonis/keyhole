type Error<'a> = nom::error::Error<&'a [u8]>;
use core::str;

use nom::bytes::complete::take;
use nom::IResult;

#[derive(Default, Debug)]
pub struct RDB<'a> {
    magic: &'a [u8],
    version: u32,
    auxiliary_commands: Vec<AuxiliaryField>,
}

#[derive(Default, Debug, Clone)]
struct AuxiliaryField {
    opcode: u8,
    key: String,
    value: String,
}

impl<'a> RDB<'a> {
    pub fn new(data: &'a [u8]) -> RDB<'a> {
        let mut r = RDB {
            ..Default::default()
        };

        r.parse(data).expect("why fail?");

        r
    }

    pub fn parse(
        &mut self,
        data: &'a [u8],
    ) -> Result<(), nom::error::Error<nom::error::ErrorKind>> {
        let (remaining, magic) = RDB::parse_magic(data).unwrap();
        println!(
            "Magic: {}",
            str::from_utf8(magic).expect("this should be REDIS")
        );

        self.magic = magic;

        let (remaining, version) = RDB::parse_version(remaining).unwrap();
        println!(
            "Version: {}",
            str::from_utf8(version).expect("this should be valid ascii")
        );

        let version = std::str::from_utf8(&version)
            .unwrap()
            .parse::<u32>()
            .unwrap();

        self.version = version;

        let mut remaining_bytes = remaining;
        loop {
            let (remaining, opcode) = RDB::parse_auxiliary_field(remaining_bytes).unwrap();
            match opcode {
                // Auxiliary Field
                0xFA => {
                    let (remaining, k) = self.parse_rstring(remaining).unwrap();
                    let (remaining, v) = self.parse_rstring(remaining).unwrap();

                    self.auxiliary_commands.push(AuxiliaryField {
                        opcode,
                        key: k,
                        value: v,
                    });

                    remaining_bytes = remaining;
                }
                _ => {
                    println!("Aux Field New: {:X?}", opcode);
                    break;
                }
            }
        }
        Ok(())
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

    fn get_length_encoded_string(input: &[u8], length: u8) -> IResult<&[u8], String> {
        let mut remainder = input;
        let le = (length & 0b11000000 as u8) >> 6;

        println!("Length Encoding Flags: {:?}", le);
        let str_value = match le {
            0b00 => {
                // 00   The next 6 bits represent the length
                let length = length & 0b00111111;
                let bytes;
                (remainder, bytes) = take::<usize, &[u8], ()>(length as usize)(remainder).unwrap();
                std::str::from_utf8(bytes)
                    .expect("this should be UTF-8")
                    .to_string()
            }
            0b01 => {
                // 01	Read one additional byte. The combined 14 bits represent the length
                let size;
                (remainder, size) = nom::number::complete::u8(input)?;

                let length = size as u64 + (length as u64) << 8;
                let bytes;
                (remainder, bytes) = take::<usize, &[u8], ()>(length as usize)(remainder).unwrap();

                std::str::from_utf8(bytes)
                    .expect("this should be UTF-8")
                    .to_string()
            }
            0b10 => {
                // 10	Discard the remaining 6 bits. The next 4 bytes from the stream represent the length
                let length;
                (remainder, length) =
                    nom::number::complete::u64(nom::number::Endianness::Big)(input)?;
                let bytes;
                (remainder, bytes) = take::<usize, &[u8], ()>(length as usize)(remainder).unwrap();

                std::str::from_utf8(bytes)
                    .expect("this should be UTF-8")
                    .to_string()
            }
            0b11 => {
                // 11	The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
                let format = length & 0b00111111;
                match format {
                    0 => {
                        let val;
                        (remainder, val) = nom::number::complete::u8::<&[u8], ()>(remainder)
                            .expect("valid int needed");
                        format!("{}", val)
                    }
                    1 => {
                        let val;
                        (remainder, val) = nom::number::complete::u16::<&[u8], ()>(
                            nom::number::Endianness::Big,
                        )(remainder)
                        .expect("valid int needed");
                        format!("{}", val)
                    }
                    2 => {
                        let val;
                        (remainder, val) = nom::number::complete::u32::<&[u8], ()>(
                            nom::number::Endianness::Big,
                        )(remainder)
                        .expect("valid int needed");
                        format!("{}", val)
                    }
                    _ => {
                        todo!();
                    }
                }
            }
            _ => unimplemented!(),
        }
        .to_string();

        Ok((remainder, str_value))
    }

    fn parse_rstring<'b>(&mut self, input: &'b [u8]) -> Result<(&'b [u8], String), Error> {
        let (remainder, length) = nom::number::complete::u8::<&[u8], ()>(input).unwrap();

        let (remainder, val) = Self::get_length_encoded_string(remainder, length).unwrap();
        Ok((remainder, val))
    }
}
