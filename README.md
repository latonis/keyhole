# keyhole

A Redis RDB (Redis Database) file parser written in Rust.

## Overview

`keyhole` is a library for parsing Redis RDB files, which are the snapshot files that Redis creates when persisting data to disk. This library provides a streaming parser that reads RDB files and extracts metadata such as:

- RDB version
- Magic string (should be "REDIS")
- Auxiliary fields (global configuration like `redis-version`, `redis-bits`, etc.)

## Features

- Pure Rust implementation using the `nom` parsing library
- Streaming parser that processes RDB files byte by byte
- Lifetime-aware parsing with zero-copy string references where possible
- Comprehensive encoding support including:
  - Length-encoded strings (6-bit, 14-bit, 32-bit, 64-bit lengths)
  - Integer encodings (8-bit, 16-bit, 32-bit, 64-bit)
  - Compressed strings (LZF compression)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
keyhole = "0.1.0"
nom = "7.1.3"
```

## Usage

```rust
use keyhole::RDB;

fn main() -> std::io::Result<()> {
    // Load an RDB file
    let data = std::fs::read("dump.rdb")?;
    
    // Parse the RDB file
    let rdb = RDB::new(&data);
    
    println!("Magic: {:?}", std::str::from_utf8(rdb.magic)?);
    println!("Version: {}", rdb.version);
    
    for aux in &rdb.auxiliary_commands {
        println!("{} = {}", aux.key, aux.value);
    }
    
    Ok(())
}
```

## RDB File Format

The Redis RDB format is a binary format consisting of:

1. Magic string (5 bytes): `REDIS`
2. RDB version (4 bytes): ASCII-encoded version number
3. Database contents: Keys, values, and metadata
4. Auxiliary fields: Global metadata (opcode `0xFA`)
5. End marker (1 byte): `0xFF`

### Supported Opcodes

| Opcode | Name | Description |
|--------|------|-------------|
| `0xFA` | AUX | Auxiliary field (key-value metadata) |
| `0xFF` | EOF | End of file marker |

### String Encodings

Strings in RDB files can be encoded in several ways:

| Encoding | Description |
|----------|-------------|
| `00xxxxxx` | 6-bit length (0-63 bytes) |
| `01xxxxxx` | 14-bit length (uses next byte + 6 bits) |
| `10xxxxxx` | 32-bit length (big-endian) |
| `11xxxxxx` | Special encoding (integers, LZF compressed) |

## Command Line Usage

The project includes a binary example:

```bash
cargo run --bin keyhole
```

This will parse `tests/dump.rdb` and output the parsed data.

## Project Structure

```
keyhole/
├── src/
│   ├── lib.rs      # Public API exports
│   ├── parser.rs   # Parsing logic and types
│   └── bin/
│       └── main.rs # CLI entry point
├── tests/
│   └── dump.rdb    # Sample RDB file for testing
├── Cargo.toml
└── README.md
```

## Dependencies

- `nom` 7.1.3 - Parser combinator library

## License

This project is licensed under the [MIT license](LICENSE).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## See Also

- [Redis RDB Format Specification](https://github.com/redis/redis/blob/unstable/src/rdb.h)
- [Redis Open Source](https://github.com/redis/redis)
