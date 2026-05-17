use core::str;
use std::borrow::Cow;

use nom::bytes::complete::take;
use nom::IResult;

// Length-encoding prefixes (top 2 bits of the first byte)
// 00|XXXXXX  → next 6 bits are the length
// 01|XXXXXX  → 14-bit length: 6 bits here + 8 bits in next byte (big-endian)
// 10|000000  → discard 6 bits; next 4 bytes are the length (big-endian u32)
// 10|000001  → discard 6 bits; next 8 bytes are the length (big-endian u64)
// 11|XXXXXX  → special encoding; lower 6 bits select the format
const RDB_6BITLEN: u8 = 0b00;
const RDB_14BITLEN: u8 = 0b01;
// 0b10 sub-cases are distinguished by the lower 6 bits
const RDB_32BITLEN_SUB: u8 = 0x00;
const RDB_64BITLEN_SUB: u8 = 0x01;
const RDB_ENCVAL: u8 = 0b11;

// Integer/blob sub-encodings (lower 6 bits when prefix == RDB_ENCVAL)
const RDB_ENC_INT8: u8 = 0;  //  8-bit signed integer
const RDB_ENC_INT16: u8 = 1; // 16-bit signed integer, little-endian
const RDB_ENC_INT32: u8 = 2; // 32-bit signed integer, little-endian
const RDB_ENC_LZF: u8 = 3;   // LZF-compressed blob

// Object type bytes (appear directly in the stream, not opcodes)
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_HASH_LISTPACK: u8 = 16;

// Special RDB opcodes
const RDB_OPCODE_KEY_META: u8 = 243;
const RDB_OPCODE_SLOT_INFO: u8 = 244;
const RDB_OPCODE_FUNCTION2: u8 = 245;
const RDB_OPCODE_FUNCTION_PRE_GA: u8 = 246;
const RDB_OPCODE_MODULE_AUX: u8 = 247;
const RDB_OPCODE_IDLE: u8 = 248;
const RDB_OPCODE_FREQ: u8 = 249;
const RDB_OPCODE_AUX: u8 = 250;
const RDB_OPCODE_RESIZEDB: u8 = 251;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
const RDB_OPCODE_EXPIRETIME: u8 = 253;
const RDB_OPCODE_SELECTDB: u8 = 254;
const RDB_OPCODE_EOF: u8 = 255;

/// A key/value pair from an RDB auxiliary field.
///
/// Both fields borrow directly from the input buffer when the value is stored
/// as a plain string. Integer-encoded values are the only heap allocations.
#[derive(Debug, Clone)]
pub struct AuxiliaryField<'a> {
    pub key: Cow<'a, str>,
    pub value: Cow<'a, str>,
}

/// The value half of a Redis key-value entry.
#[derive(Debug, Clone)]
pub enum Value<'a> {
    /// A plain Redis string. Borrows from the input buffer when stored as a
    /// raw string; allocates only for integer-encoded strings.
    String(Cow<'a, str>),

    /// A hash decoded from a listpack blob. Always owns its pairs because the
    /// source data is either LZF-decompressed (a new allocation) or extracted
    /// from a binary listpack format that cannot be borrowed as UTF-8.
    Hash(Vec<(String, String)>),
}

/// A single Redis key-value entry, including any per-key metadata that
/// preceded it in the RDB stream.
#[derive(Debug)]
pub struct Entry<'a> {
    pub key: Cow<'a, str>,
    pub value: Value<'a>,
    /// Absolute expiry as milliseconds since the Unix epoch.
    ///
    /// Populated from `EXPIRETIME_MS` directly, or from `EXPIRETIME` (seconds)
    /// multiplied by 1000. `None` means the key has no expiry.
    pub expires_at_ms: Option<i64>,
    /// LRU idle time in seconds, from the `IDLE` opcode.
    pub lru_idle_secs: Option<u64>,
    /// LFU frequency counter, from the `FREQ` opcode.
    pub lfu_freq: Option<u8>,
}

/// Identity, size hints, and entries for a single Redis logical database.
///
/// `id` comes from the `SELECTDB` opcode. `size` and `expires_size` come from
/// the `RESIZEDB` opcode that immediately follows it. Both size fields are
/// advisory hints Redis writes to allow pre-sizing hash tables on load; they
/// are not required for correctness.
#[derive(Debug, Default)]
pub struct Database<'a> {
    pub id: u64,
    /// Total number of keys in this database (advisory).
    pub size: u64,
    /// Number of keys that have an expiry set (advisory).
    pub expires_size: u64,
    pub entries: Vec<Entry<'a>>,
}

/// A parsed RDB file.
///
/// `magic` and plain-string values borrow directly from the input buffer.
/// Integer-encoded strings and LZF-decompressed values are the only heap
/// allocations.
#[derive(Debug)]
pub struct RDB<'a> {
    pub magic: &'a str,
    pub version: u32,
    pub auxiliary_fields: Vec<AuxiliaryField<'a>>,
    pub databases: Vec<Database<'a>>,
    /// CRC64 checksum trailing the EOF opcode. Present when RDB version >= 5.
    pub checksum: Option<u64>,
}

/// Decompress an LZF-compressed byte slice into a new `Vec<u8>`.
///
/// Implements Redis's LZF variant (`lzf_d.c`). Two control-sequence types:
///
/// - **Literal run**: `ctrl < 32` → copy `ctrl + 1` bytes verbatim.
/// - **Back reference**: `ctrl >= 32` → copy from earlier in the output.
///   - `len  = (ctrl >> 5) + 2`; if the 3-bit part equals 7, an extra byte
///     extends the length.
///   - `dist = ((ctrl & 0x1f) << 8) | dist_byte + 1`
fn lzf_decompress(input: &[u8], expected_len: usize) -> Result<Vec<u8>, ()> {
    let mut output: Vec<u8> = Vec::with_capacity(expected_len);
    let mut i = 0;

    while i < input.len() {
        let ctrl = input[i] as usize;
        i += 1;

        if ctrl < 32 {
            // Literal run: copy the next (ctrl + 1) bytes verbatim.
            let end = i + ctrl + 1;
            if end > input.len() {
                return Err(());
            }
            output.extend_from_slice(&input[i..end]);
            i = end;
        } else {
            // Back reference into the already-decompressed output.
            let mut len = ctrl >> 5;

            // Redis lzf_d.c reads bytes in this exact order:
            //   1. length extension (only when the 3-bit len field == 7)
            //   2. distance low byte (always)
            // The distance high bits come from ctrl itself, so no extra read
            // is needed for them.
            if len == 7 {
                if i >= input.len() {
                    return Err(());
                }
                len += input[i] as usize;
                i += 1;
            }
            len += 2;

            if i >= input.len() {
                return Err(());
            }
            let dist_byte = input[i] as usize;
            i += 1;

            let dist = (((ctrl & 0x1f) << 8) | dist_byte) + 1;
            let start = output.len().checked_sub(dist).ok_or(())?;

            // Iterate one byte at a time so overlapping copies work correctly:
            // each written byte is immediately available for subsequent reads.
            for j in 0..len {
                let byte = output[start + j];
                output.push(byte);
            }
        }
    }

    Ok(output)
}

/// Decode a Redis listpack blob into a flat `Vec<String>` of elements.
///
/// A listpack starts with a 4-byte `total_bytes` (u32 LE) and a 2-byte
/// `num_elements` (u16 LE) header, followed by tightly-packed entries each
/// terminated by a variable-length backlen field, and a final `0xFF` sentinel.
///
/// The returned elements alternate field → value and should be zipped into
/// pairs by the caller.
fn decode_listpack(data: &[u8]) -> Result<Vec<String>, ()> {
    if data.len() < 6 {
        return Err(());
    }

    let num_elements = u16::from_le_bytes([data[4], data[5]]) as usize;
    let mut elements: Vec<String> = Vec::with_capacity(num_elements);
    let mut pos = 6;

    while pos < data.len() {
        if data[pos] == 0xFF {
            break;
        }

        let (value, entry_size) = decode_listpack_entry(&data[pos..])?;
        elements.push(value);
        pos += entry_size + backlen_size(entry_size);
    }

    Ok(elements)
}

/// Decode a single listpack entry beginning at `data[0]`.
///
/// Returns the decoded value (as a string) and the entry's byte size
/// **excluding** the trailing backlen field.
///
/// Listpack encoding summary (from `listpack.h`):
///
/// ┌─────────────────────┬──────────────────────────────────────────────┐
/// │ Pattern             │ Type                                         │
/// ├─────────────────────┼──────────────────────────────────────────────┤
/// │ 0XXXXXXX            │ 7-bit unsigned integer (0..127)              │
/// │ 10XXXXXX            │ 6-bit string length (max 63 bytes)           │
/// │ 110XXXXX XX         │ 13-bit signed integer (-4096..4095)          │
/// │ 1110XXXX XX         │ 12-bit string length (max 4095 bytes)        │
/// │ 0xF1                │ 16-bit signed integer (little-endian)        │
/// │ 0xF2                │ 24-bit signed integer (little-endian)        │
/// │ 0xF3                │ 32-bit signed integer (little-endian)        │
/// │ 0xF4                │ 64-bit signed integer (little-endian)        │
/// │ 0xF0                │ 32-bit string length (little-endian)         │
/// └─────────────────────┴──────────────────────────────────────────────┘
fn decode_listpack_entry(data: &[u8]) -> Result<(String, usize), ()> {
    if data.is_empty() {
        return Err(());
    }
    let byte = data[0];

    if byte & 0x80 == 0 {
        // 0XXXXXXX — 7-bit unsigned integer
        Ok(((byte & 0x7F).to_string(), 1))
    } else if byte & 0xC0 == 0x80 {
        // 10XXXXXX — 6-bit string length
        let len = (byte & 0x3F) as usize;
        if data.len() < 1 + len {
            return Err(());
        }
        let s = str::from_utf8(&data[1..1 + len]).map_err(|_| ())?;
        Ok((s.to_string(), 1 + len))
    } else if byte & 0xE0 == 0xC0 {
        // 110XXXXX XXXXXXXX — 13-bit signed integer
        if data.len() < 2 {
            return Err(());
        }
        let raw = (((byte & 0x1F) as u16) << 8) | data[1] as u16;
        // Sign-extend from 13 bits: if the 13th bit is set, subtract 2^13.
        let val = if raw >= (1 << 12) {
            raw as i32 - (1 << 13)
        } else {
            raw as i32
        };
        Ok((val.to_string(), 2))
    } else if byte & 0xF0 == 0xE0 {
        // 1110XXXX XXXXXXXX — 12-bit string length
        if data.len() < 2 {
            return Err(());
        }
        let len = (((byte & 0x0F) as usize) << 8) | data[1] as usize;
        if data.len() < 2 + len {
            return Err(());
        }
        let s = str::from_utf8(&data[2..2 + len]).map_err(|_| ())?;
        Ok((s.to_string(), 2 + len))
    } else {
        match byte {
            0xF1 => {
                // 16-bit signed integer, little-endian
                if data.len() < 3 {
                    return Err(());
                }
                let val = i16::from_le_bytes([data[1], data[2]]);
                Ok((val.to_string(), 3))
            }
            0xF2 => {
                // 24-bit signed integer, little-endian.
                // Load the 3 bytes into the upper 24 bits of an i32 and
                // arithmetic-shift right by 8 to sign-extend.
                if data.len() < 4 {
                    return Err(());
                }
                let raw =
                    data[1] as u32 | (data[2] as u32) << 8 | (data[3] as u32) << 16;
                let val = ((raw << 8) as i32) >> 8;
                Ok((val.to_string(), 4))
            }
            0xF3 => {
                // 32-bit signed integer, little-endian
                if data.len() < 5 {
                    return Err(());
                }
                let val = i32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                Ok((val.to_string(), 5))
            }
            0xF4 => {
                // 64-bit signed integer, little-endian
                if data.len() < 9 {
                    return Err(());
                }
                let val = i64::from_le_bytes([
                    data[1], data[2], data[3], data[4],
                    data[5], data[6], data[7], data[8],
                ]);
                Ok((val.to_string(), 9))
            }
            0xF0 => {
                // 32-bit string length, little-endian
                if data.len() < 5 {
                    return Err(());
                }
                let len =
                    u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
                if data.len() < 5 + len {
                    return Err(());
                }
                let s = str::from_utf8(&data[5..5 + len]).map_err(|_| ())?;
                Ok((s.to_string(), 5 + len))
            }
            _ => Err(()),
        }
    }
}

/// Return the number of bytes occupied by a listpack backlen field for an
/// entry of `entry_size` bytes. Mirrors Redis's `lpEncodeBacklen` sizing.
fn backlen_size(entry_size: usize) -> usize {
    if entry_size < 128 {
        1
    } else if entry_size < 16_384 {
        2
    } else if entry_size < 2_097_152 {
        3
    } else if entry_size < 268_435_456 {
        4
    } else {
        5
    }
}

impl<'a> RDB<'a> {
    /// Parse an RDB file from a byte slice. Returns an error string on failure.
    pub fn new(data: &'a [u8]) -> Result<RDB<'a>, String> {
        let (_, rdb) =
            Self::parse(data).map_err(|e| format!("Failed to parse RDB: {:?}", e))?;
        Ok(rdb)
    }

    fn parse(input: &'a [u8]) -> IResult<&'a [u8], RDB<'a>> {
        // Magic: "REDIS" (5 bytes)
        let (remaining, magic_bytes) = take(5usize)(input)?;
        let magic = str::from_utf8(magic_bytes).map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(
                magic_bytes,
                nom::error::ErrorKind::Verify,
            ))
        })?;
        if magic != "REDIS" {
            return Err(nom::Err::Failure(nom::error::Error::new(
                magic_bytes,
                nom::error::ErrorKind::Tag,
            )));
        }

        // Version: 4 ASCII digits
        let (remaining, version_bytes) = take(4usize)(remaining)?;
        let version = str::from_utf8(version_bytes)
            .map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(
                    version_bytes,
                    nom::error::ErrorKind::Verify,
                ))
            })?
            .parse::<u32>()
            .map_err(|_| {
                nom::Err::Failure(nom::error::Error::new(
                    version_bytes,
                    nom::error::ErrorKind::Verify,
                ))
            })?;

        let mut remaining = remaining;
        let mut auxiliary_fields: Vec<AuxiliaryField<'a>> = Vec::new();
        let mut databases: Vec<Database<'a>> = Vec::new();
        let mut checksum: Option<u64> = None;

        // Per-key metadata opcodes are emitted immediately before the type
        // byte of the entry they annotate. Hold them here and consume them
        // when the next entry is pushed.
        let mut pending_expire_ms: Option<i64> = None;
        let mut pending_idle: Option<u64> = None;
        let mut pending_freq: Option<u8> = None;

        loop {
            let (rest, opcode) = nom::number::complete::u8(remaining)?;
            match opcode {
                // Global metadata
                RDB_OPCODE_AUX => {
                    let (rest, key) = Self::parse_rstring(rest)?;
                    let (rest, value) = Self::parse_rstring(rest)?;
                    auxiliary_fields.push(AuxiliaryField { key, value });
                    remaining = rest;
                }

                // Database selection
                RDB_OPCODE_SELECTDB => {
                    let (rest, db_id) = Self::parse_length(rest)?;
                    databases.push(Database {
                        id: db_id,
                        size: 0,
                        expires_size: 0,
                        entries: Vec::new(),
                    });
                    remaining = rest;
                }

                // RESIZEDB always immediately follows SELECTDB, so
                // `databases.last_mut()` is always the current database.
                RDB_OPCODE_RESIZEDB => {
                    let (rest, db_size) = Self::parse_length(rest)?;
                    let (rest, expires_size) = Self::parse_length(rest)?;
                    if let Some(db) = databases.last_mut() {
                        db.size = db_size;
                        db.expires_size = expires_size;
                    }
                    remaining = rest;
                }

                // Expire time in seconds — 4-byte little-endian int32.
                // Redis stores time_t as int32_t for this opcode.
                // Converted to milliseconds so expires_at_ms is always one unit.
                RDB_OPCODE_EXPIRETIME => {
                    let (rest, ts) =
                        nom::number::complete::i32(nom::number::Endianness::Little)(rest)?;
                    pending_expire_ms = Some(ts as i64 * 1000);
                    remaining = rest;
                }

                // Expire time in milliseconds — 8-byte little-endian int64.
                // Redis stores this as int64_t (rdbLoadMillisecondTime in rdb.c).
                RDB_OPCODE_EXPIRETIME_MS => {
                    let (rest, ts) =
                        nom::number::complete::i64(nom::number::Endianness::Little)(rest)?;
                    pending_expire_ms = Some(ts);
                    remaining = rest;
                }

                // LRU idle time in seconds — length-encoded integer.
                RDB_OPCODE_IDLE => {
                    let (rest, idle) = Self::parse_length(rest)?;
                    pending_idle = Some(idle);
                    remaining = rest;
                }

                // LFU frequency counter — single raw byte.
                RDB_OPCODE_FREQ => {
                    let (rest, freq) = nom::number::complete::u8(rest)?;
                    pending_freq = Some(freq);
                    remaining = rest;
                }

                // Key-value entries
                RDB_TYPE_STRING | RDB_TYPE_HASH_LISTPACK => {
                    let (rest, key) = Self::parse_rstring(rest)?;
                    let (rest, value) = Self::parse_value(rest, opcode)?;
                    if let Some(db) = databases.last_mut() {
                        db.entries.push(Entry {
                            key,
                            value,
                            expires_at_ms: pending_expire_ms.take(),
                            lru_idle_secs: pending_idle.take(),
                            lfu_freq: pending_freq.take(),
                        });
                    }
                    remaining = rest;
                }

                // End of file
                RDB_OPCODE_EOF => {
                    // An 8-byte CRC64 checksum is appended in RDB version >= 5.
                    if version >= 5 {
                        let (rest, crc) =
                            nom::number::complete::u64(nom::number::Endianness::Little)(rest)?;
                        checksum = Some(crc);
                        remaining = rest;
                    } else {
                        remaining = rest;
                    }
                    break;
                }

                // TODO: Not yet implemented
                RDB_OPCODE_KEY_META
                | RDB_OPCODE_SLOT_INFO
                | RDB_OPCODE_FUNCTION2
                | RDB_OPCODE_FUNCTION_PRE_GA
                | RDB_OPCODE_MODULE_AUX => {
                    remaining = rest;
                    break;
                }

                _ => {
                    // Unimplemented object-type opcode — stop processing.
                    remaining = rest;
                    break;
                }
            }
        }

        Ok((remaining, RDB { magic, version, auxiliary_fields, databases, checksum }))
    }

    /// Dispatch to the correct value parser for `type_byte`.
    fn parse_value(input: &'a [u8], type_byte: u8) -> IResult<&'a [u8], Value<'a>> {
        match type_byte {
            RDB_TYPE_STRING => {
                let (remaining, s) = Self::parse_rstring(input)?;
                Ok((remaining, Value::String(s)))
            }
            RDB_TYPE_HASH_LISTPACK => {
                // The value is a listpack blob, potentially LZF-compressed.
                let (remaining, blob) = Self::parse_bytes(input)?;
                let elements = decode_listpack(&blob).map_err(|_| {
                    nom::Err::Failure(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Verify,
                    ))
                })?;

                // Listpack elements alternate field → value → field → value …
                //
                // ┌─────────┬─────────┬─────────┬─────────┬─────────┬─────────┐
                // │ field1  │ value1  │ field2  │ value2  │ field3  │ value3  │
                // └─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
                //      │         │         │         │         │         │
                //      └────┬────┘         └────┬────┘         └────┬────┘
                //           ▼                   ▼                   ▼
                //     (field1, value1)     (field2, value2)     (field3, value3)
                //
                let pairs = elements
                    .chunks(2)
                    .filter_map(|c| {
                        if c.len() == 2 {
                            Some((c[0].clone(), c[1].clone()))
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok((remaining, Value::Hash(pairs)))
            }
            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Switch,
            ))),
        }
    }

    /// Decode a length-encoded integer, returning the raw `u64` value.
    ///
    /// Used by `SELECTDB`, `RESIZEDB`, and `IDLE`. Distinct from
    /// `parse_rstring` / `decode_length_encoded_string`, which consume the
    /// length prefix and then read that many bytes as a string. Hard-errors on
    /// `RDB_ENCVAL` (0b11) since a special string encoding is never a valid
    /// plain length.
    fn parse_length(input: &[u8]) -> IResult<&[u8], u64> {
        let (remaining, first) = nom::number::complete::u8(input)?;
        let prefix = (first & 0b1100_0000) >> 6;

        match prefix {
            RDB_6BITLEN => Ok((remaining, (first & 0b0011_1111) as u64)),

            RDB_14BITLEN => {
                let (remaining, next) = nom::number::complete::u8(remaining)?;
                let len = (((first & 0b0011_1111) as u64) << 8) | next as u64;
                Ok((remaining, len))
            }

            0b10 => {
                let sub_type = first & 0b0011_1111;
                match sub_type {
                    RDB_32BITLEN_SUB => {
                        let (remaining, len) =
                            nom::number::complete::u32(nom::number::Endianness::Big)(remaining)?;
                        Ok((remaining, len as u64))
                    }
                    RDB_64BITLEN_SUB => {
                        nom::number::complete::u64(nom::number::Endianness::Big)(remaining)
                    }
                    _ => Err(nom::Err::Failure(nom::error::Error::new(
                        input,
                        nom::error::ErrorKind::Switch,
                    ))),
                }
            }

            _ => Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Switch,
            ))),
        }
    }

    /// Read the length-prefix byte and then decode the string it describes.
    fn parse_rstring(input: &'a [u8]) -> IResult<&'a [u8], Cow<'a, str>> {
        let (remaining, length) = nom::number::complete::u8(input)?;
        Self::decode_length_encoded_string(remaining, length)
    }

    /// Read the length-prefix byte and decode the raw bytes it describes.
    ///
    /// Unlike `parse_rstring`, this handles `RDB_ENC_LZF` by decompressing
    /// into an owned `Vec<u8>`. Used for blob value types such as
    /// `RDB_TYPE_HASH_LISTPACK` where the payload is binary, not text.
    fn parse_bytes(input: &'a [u8]) -> IResult<&'a [u8], Cow<'a, [u8]>> {
        let (remaining, length) = nom::number::complete::u8(input)?;
        Self::decode_length_encoded_bytes(remaining, length)
    }

    /// Decode a length-encoded string given the already-consumed prefix byte.
    ///
    /// Returns `Cow::Borrowed` for plain strings in the input buffer and
    /// `Cow::Owned` for integer-encoded values where no contiguous string
    /// representation exists in the buffer.
    fn decode_length_encoded_string(
        input: &'a [u8],
        length: u8,
    ) -> IResult<&'a [u8], Cow<'a, str>> {
        let prefix = (length & 0b1100_0000) >> 6;
        let mut remaining = input;

        let value = match prefix {
            // 6-bit length
            RDB_6BITLEN => {
                let len = (length & 0b0011_1111) as usize;
                let (new_remaining, s) = Self::take_utf8_str(remaining, len)?;
                remaining = new_remaining;
                Cow::Borrowed(s)
            }

            // 14-bit length
            // The 6 lower bits of the first byte are the HIGH bits;
            // the next byte is the LOW byte — big-endian 14-bit value.
            RDB_14BITLEN => {
                let next_byte;
                (remaining, next_byte) = nom::number::complete::u8(remaining)?;
                let len = (((length & 0b0011_1111) as usize) << 8) | (next_byte as usize);
                let (new_remaining, s) = Self::take_utf8_str(remaining, len)?;
                remaining = new_remaining;
                Cow::Borrowed(s)
            }

            // 32-bit or 64-bit length (net byte order)
            0b10 => {
                let sub_type = length & 0b0011_1111;
                match sub_type {
                    RDB_32BITLEN_SUB => {
                        let len;
                        (remaining, len) =
                            nom::number::complete::u32(nom::number::Endianness::Big)(remaining)?;
                        let (new_remaining, s) = Self::take_utf8_str(remaining, len as usize)?;
                        remaining = new_remaining;
                        Cow::Borrowed(s)
                    }
                    RDB_64BITLEN_SUB => {
                        let len;
                        (remaining, len) =
                            nom::number::complete::u64(nom::number::Endianness::Big)(remaining)?;
                        let len_usize = usize::try_from(len).map_err(|_| {
                            nom::Err::Failure(nom::error::Error::new(
                                input,
                                nom::error::ErrorKind::TooLarge,
                            ))
                        })?;
                        let (new_remaining, s) = Self::take_utf8_str(remaining, len_usize)?;
                        remaining = new_remaining;
                        Cow::Borrowed(s)
                    }
                    _ => {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Switch,
                        )));
                    }
                }
            }

            // Integer-encoded values have no contiguous string representation
            // in the buffer, so an allocation is unavoidable.
            RDB_ENCVAL => {
                let format = length & 0b0011_1111;
                match format {
                    RDB_ENC_INT8 => {
                        let val;
                        (remaining, val) = nom::number::complete::i8(remaining)?;
                        Cow::Owned(val.to_string())
                    }
                    RDB_ENC_INT16 => {
                        let val;
                        (remaining, val) =
                            nom::number::complete::i16(nom::number::Endianness::Little)(remaining)?;
                        Cow::Owned(val.to_string())
                    }
                    RDB_ENC_INT32 => {
                        let val;
                        (remaining, val) =
                            nom::number::complete::i32(nom::number::Endianness::Little)(remaining)?;
                        Cow::Owned(val.to_string())
                    }
                    _ => {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Switch,
                        )));
                    }
                }
            }

            _ => unreachable!("2-bit prefix can only be 0b00, 0b01, 0b10, or 0b11"),
        };

        Ok((remaining, value))
    }

    /// Decode a length-encoded byte blob given the already-consumed prefix byte.
    ///
    /// Returns `Cow::Borrowed` for plain blobs (zero-copy slice of the input)
    /// and `Cow::Owned` for LZF-compressed blobs (decompressed into a new
    /// `Vec<u8>`). Integer encodings are rejected — they produce no binary
    /// payload.
    fn decode_length_encoded_bytes(
        input: &'a [u8],
        length: u8,
    ) -> IResult<&'a [u8], Cow<'a, [u8]>> {
        let prefix = (length & 0b1100_0000) >> 6;
        let mut remaining = input;

        let bytes = match prefix {
            RDB_6BITLEN => {
                let len = (length & 0b0011_1111) as usize;
                let b;
                (remaining, b) = take(len)(remaining)?;
                Cow::Borrowed(b)
            }

            RDB_14BITLEN => {
                let next_byte;
                (remaining, next_byte) = nom::number::complete::u8(remaining)?;
                let len = (((length & 0b0011_1111) as usize) << 8) | next_byte as usize;
                let b;
                (remaining, b) = take(len)(remaining)?;
                Cow::Borrowed(b)
            }

            0b10 => {
                let sub_type = length & 0b0011_1111;
                match sub_type {
                    RDB_32BITLEN_SUB => {
                        let len;
                        (remaining, len) =
                            nom::number::complete::u32(nom::number::Endianness::Big)(remaining)?;
                        let b;
                        (remaining, b) = take(len as usize)(remaining)?;
                        Cow::Borrowed(b)
                    }
                    RDB_64BITLEN_SUB => {
                        let len;
                        (remaining, len) =
                            nom::number::complete::u64(nom::number::Endianness::Big)(remaining)?;
                        let len_usize = usize::try_from(len).map_err(|_| {
                            nom::Err::Failure(nom::error::Error::new(
                                input,
                                nom::error::ErrorKind::TooLarge,
                            ))
                        })?;
                        let b;
                        (remaining, b) = take(len_usize)(remaining)?;
                        Cow::Borrowed(b)
                    }
                    _ => {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Switch,
                        )));
                    }
                }
            }

            RDB_ENCVAL => {
                let format = length & 0b0011_1111;
                match format {
                    RDB_ENC_LZF => {
                        let (rest, clen) = Self::parse_length(remaining)?;
                        let (rest, ulen) = Self::parse_length(rest)?;
                        let clen_usize = usize::try_from(clen).map_err(|_| {
                            nom::Err::Failure(nom::error::Error::new(
                                input,
                                nom::error::ErrorKind::TooLarge,
                            ))
                        })?;
                        let ulen_usize = usize::try_from(ulen).map_err(|_| {
                            nom::Err::Failure(nom::error::Error::new(
                                input,
                                nom::error::ErrorKind::TooLarge,
                            ))
                        })?;
                        let (rest, compressed) = take(clen_usize)(rest)?;
                        let decompressed = lzf_decompress(compressed, ulen_usize)
                            .map_err(|_| {
                                nom::Err::Failure(nom::error::Error::new(
                                    input,
                                    nom::error::ErrorKind::Verify,
                                ))
                            })?;
                        remaining = rest;
                        Cow::Owned(decompressed)
                    }
                    // Integer encodings do not produce binary blobs.
                    _ => {
                        return Err(nom::Err::Failure(nom::error::Error::new(
                            input,
                            nom::error::ErrorKind::Switch,
                        )));
                    }
                }
            }

            _ => unreachable!("2-bit prefix can only be 0b00, 0b01, 0b10, or 0b11"),
        };

        Ok((remaining, bytes))
    }

    fn take_utf8_str(remaining: &'a [u8], len: usize) -> IResult<&'a [u8], &'a str> {
        let (remaining, bytes) = take(len)(remaining)?;
        let s = str::from_utf8(bytes).map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(bytes, nom::error::ErrorKind::Verify))
        })?;
        Ok((remaining, s))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lzf_single_literal_byte() {
        // ctrl = 0x00 → literal run of 1 byte
        let compressed = [0x00, 0xAB];
        assert_eq!(lzf_decompress(&compressed, 1), Ok(vec![0xAB]));
    }

    #[test]
    fn lzf_multiple_literal_bytes() {
        // ctrl = 0x02 → literal run of 3 bytes
        let compressed = [0x02, b'a', b'b', b'c'];
        assert_eq!(lzf_decompress(&compressed, 3), Ok(vec![b'a', b'b', b'c']));
    }

    #[test]
    fn lzf_back_reference_no_overlap() {
        // Literal "abc", then copy 3 bytes from dist=3 → "abcabc"
        // len_part=1 → ctrl = (1 << 5) | 0 = 0x20
        // dist=3 → dist_byte = dist - 1 = 2
        let compressed = [0x02, b'a', b'b', b'c', 0x20, 0x02];
        assert_eq!(
            lzf_decompress(&compressed, 6),
            Ok(vec![b'a', b'b', b'c', b'a', b'b', b'c'])
        );
    }

    #[test]
    fn lzf_back_reference_overlapping() {
        // Literal "ab", then copy 4 bytes from dist=2 → "ababab"
        // Each copied byte is immediately available for the next copy.
        // len_part=2 → ctrl = (2 << 5) | 0 = 0x40; dist=2 → dist_byte = 1
        let compressed = [0x01, b'a', b'b', 0x40, 0x01];
        assert_eq!(
            lzf_decompress(&compressed, 6),
            Ok(vec![b'a', b'b', b'a', b'b', b'a', b'b'])
        );
    }

    #[test]
    fn lzf_back_reference_with_length_extension() {
        // len_part == 7 triggers the extension byte, which is read BEFORE the
        // distance byte (matching Redis's lzf_d.c).
        // Build: literal "abcdefgh" (8 bytes), then back-ref len=10 dist=8.
        // len_part=7+1ext → ctrl = (7 << 5) | 0 = 0xE0; ext=1 (len=7+1+2=10);
        // dist=8 → dist_byte = 7
        let mut compressed = vec![0x07]; // literal run of 8 bytes
        compressed.extend_from_slice(b"abcdefgh");
        compressed.push(0xE0); // ctrl: len_part=7, dist_high=0
        compressed.push(0x01); // length extension: len = 7+1+2 = 10
        compressed.push(0x07); // dist_byte: dist = (0|7)+1 = 8
        let result = lzf_decompress(&compressed, 18).unwrap();
        assert_eq!(&result[..8], b"abcdefgh");
        assert_eq!(&result[8..], b"abcdefghab"); // 10 bytes from dist=8
    }

    #[test]
    fn lzf_truncated_returns_error() {
        // ctrl byte says literal run of 4 but only 2 bytes follow
        let compressed = [0x03, b'a', b'b'];
        assert!(lzf_decompress(&compressed, 4).is_err());
    }

    #[test]
    fn lzf_back_reference_out_of_bounds_returns_error() {
        // dist is larger than output produced so far
        let compressed = [0x00, b'x', 0x20, 0x05]; // backref dist=6, only 1 byte of output
        assert!(lzf_decompress(&compressed, 4).is_err());
    }

    #[test]
    fn listpack_entry_7bit_uint() {
        // 0XXXXXXX — 7-bit unsigned integer, value 42
        let data = [42u8, 1]; // entry byte + backlen
        let (val, size) = decode_listpack_entry(&data).unwrap();
        assert_eq!(val, "42");
        assert_eq!(size, 1);
    }

    #[test]
    fn listpack_entry_6bit_string() {
        // 10XXXXXX — 6-bit length, then string bytes
        let data = [0x83, b'f', b'o', b'o', 4u8]; // len=3, "foo", backlen=4
        let (val, size) = decode_listpack_entry(&data).unwrap();
        assert_eq!(val, "foo");
        assert_eq!(size, 4); // 1 header + 3 data
    }

    #[test]
    fn listpack_entry_13bit_positive_int() {
        // 110XXXXX XXXXXXXX — 13-bit signed, value 5
        let data = [0xC0, 0x05, 1u8]; // (0 << 8) | 5 = 5, backlen
        let (val, size) = decode_listpack_entry(&data).unwrap();
        assert_eq!(val, "5");
        assert_eq!(size, 2);
    }

    #[test]
    fn listpack_entry_13bit_negative_int() {
        // -5 in 13-bit two's complement = 8192 - 5 = 8187 = 0x1FFB
        // byte0 = 0xC0 | (0x1FFB >> 8) = 0xDF; byte1 = 0xFB
        let data = [0xDF, 0xFB, 2u8];
        let (val, size) = decode_listpack_entry(&data).unwrap();
        assert_eq!(val, "-5");
        assert_eq!(size, 2);
    }

    #[test]
    fn listpack_entry_16bit_int() {
        // 0xF1 followed by 2 LE bytes
        let data = [0xF1, 0x05, 0x00, 3u8];
        let (val, size) = decode_listpack_entry(&data).unwrap();
        assert_eq!(val, "5");
        assert_eq!(size, 3);
    }

    #[test]
    fn listpack_entry_32bit_int() {
        // 0xF3 followed by 4 LE bytes
        let data = [0xF3, 0x05, 0x00, 0x00, 0x00, 5u8];
        let (val, size) = decode_listpack_entry(&data).unwrap();
        assert_eq!(val, "5");
        assert_eq!(size, 5);
    }

    #[test]
    fn listpack_entry_64bit_int() {
        // 0xF4 followed by 8 LE bytes, value = -1
        let data = [0xF4, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 9u8];
        let (val, size) = decode_listpack_entry(&data).unwrap();
        assert_eq!(val, "-1");
        assert_eq!(size, 9);
    }

    #[test]
    fn listpack_two_strings() {
        // total_bytes = 17: 6 header + (1+3+1)*2 entries + 1 terminator
        let lp = [
            17, 0, 0, 0, // total_bytes = 17
            2, 0,         // num_elements = 2
            0x83, b'f', b'o', b'o', 4, // "foo", backlen=4
            0x83, b'b', b'a', b'r', 4, // "bar", backlen=4
            0xFF,
        ];
        assert_eq!(decode_listpack(&lp).unwrap(), vec!["foo", "bar"]);
    }

    #[test]
    fn listpack_integer_entry() {
        // total_bytes = 9: 6 header + (1+1) entry + 1 terminator
        let lp = [
            9, 0, 0, 0, // total_bytes = 9
            1, 0,        // num_elements = 1
            42, 1,       // 7-bit uint = 42, backlen = 1
            0xFF,
        ];
        assert_eq!(decode_listpack(&lp).unwrap(), vec!["42"]);
    }

    #[test]
    fn listpack_too_short_returns_error() {
        assert!(decode_listpack(&[1, 2, 3]).is_err());
    }

    #[test]
    fn backlen_size_one_byte_boundaries() {
        assert_eq!(backlen_size(1), 1);
        assert_eq!(backlen_size(127), 1);
    }

    #[test]
    fn backlen_size_two_byte_boundaries() {
        assert_eq!(backlen_size(128), 2);
        assert_eq!(backlen_size(16_383), 2);
    }

    #[test]
    fn backlen_size_three_byte_boundaries() {
        assert_eq!(backlen_size(16_384), 3);
        assert_eq!(backlen_size(2_097_151), 3);
    }

    #[test]
    fn backlen_size_four_byte_boundaries() {
        assert_eq!(backlen_size(2_097_152), 4);
        assert_eq!(backlen_size(268_435_455), 4);
    }

    #[test]
    fn backlen_size_five_byte_boundary() {
        assert_eq!(backlen_size(268_435_456), 5);
    }
}
