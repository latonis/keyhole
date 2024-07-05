type Error<'a> = nom::error::Error<&'a [u8]>;
use nom::bytes::complete::take;
use nom::IResult;

#[derive(Default)]
struct RDB<'a> {
    magic: &'a [u8],
    version: u32,
    auxiliary_commands: Vec<AuxiliaryCommand>,
}

struct AuxiliaryCommand {
    opcode: u8,
}

impl<'a> RDB<'a> {
    fn parse(data: &'a [u8]) -> Result<Self, nom::error::Error<nom::error::ErrorKind>> {
        let (remaining, magic) = RDB::parse_magic(data).unwrap();
        Ok(RDB {
            magic,
            ..Default::default()
        })
    }

    fn parse_magic(input: &'a [u8]) -> IResult<&[u8], &[u8]> {
        take(5usize)(input)
    }
}
