pub mod parser;

#[derive(Default, Debug)]
pub struct RDB<'a> {
    pub magic: &'a [u8],
    pub version: u32,
    pub auxiliary_commands: Vec<AuxiliaryField>,
}

#[derive(Default, Debug, Clone)]
pub struct AuxiliaryField {
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
}
