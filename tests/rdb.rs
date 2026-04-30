use keyhole::{RDB, Value};
use std::fs;

fn load_dump() -> Vec<u8> {
    fs::read("tests/dump.rdb").expect("tests/dump.rdb not found")
}

#[test]
fn test_magic_is_redis() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.magic, "REDIS");
}

#[test]
fn test_version_is_correct() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.version, 11);
}

#[test]
fn test_aux_field_count() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.auxiliary_fields.len(), 5);
}

#[test]
fn test_aux_field_redis_ver() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    let field = rdb
        .auxiliary_fields
        .iter()
        .find(|f| f.key.as_ref() == "redis-ver")
        .expect("redis-ver not found");
    assert_eq!(field.value.as_ref(), "7.2.5");
}

#[test]
fn test_aux_field_redis_bits() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    let field = rdb
        .auxiliary_fields
        .iter()
        .find(|f| f.key.as_ref() == "redis-bits")
        .expect("redis-bits not found");
    assert_eq!(field.value.as_ref(), "64");
}

#[test]
fn test_aux_field_aof_base() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    let field = rdb
        .auxiliary_fields
        .iter()
        .find(|f| f.key.as_ref() == "aof-base")
        .expect("aof-base not found");
    assert_eq!(field.value.as_ref(), "0");
}


#[test]
fn test_database_count() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.databases.len(), 1);
}

#[test]
fn test_database_id() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.databases[0].id, 0);
}

#[test]
fn test_database_size_hint() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.databases[0].size, 1);
    assert_eq!(rdb.databases[0].expires_size, 0);
}

#[test]
fn test_database_entry_count() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.databases[0].entries.len(), 1);
}

#[test]
fn test_entry_key() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.databases[0].entries[0].key.as_ref(), "test123");
}

#[test]
fn test_entry_no_expiry() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert!(rdb.databases[0].entries[0].expires_at_ms.is_none());
}

#[test]
fn test_entry_no_lru() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert!(rdb.databases[0].entries[0].lru_idle_secs.is_none());
}

#[test]
fn test_entry_no_lfu() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert!(rdb.databases[0].entries[0].lfu_freq.is_none());
}

#[test]
fn test_entry_is_hash() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert!(
        matches!(&rdb.databases[0].entries[0].value, Value::Hash(_)),
        "expected Value::Hash"
    );
}


fn hash_pairs<'a>(rdb: &'a RDB<'a>) -> &'a Vec<(String, String)> {
    match &rdb.databases[0].entries[0].value {
        Value::Hash(pairs) => pairs,
        _ => panic!("expected Value::Hash"),
    }
}

#[test]
fn test_hash_pair_count() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(hash_pairs(&rdb).len(), 4);
}

#[test]
fn test_hash_pair_foo_bar() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    let pairs = hash_pairs(&rdb);
    assert!(
        pairs.iter().any(|(k, v)| k == "foo" && v == "bar"),
        "foo → bar not found in {pairs:?}"
    );
}

#[test]
fn test_hash_pair_bar_foo() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    let pairs = hash_pairs(&rdb);
    assert!(
        pairs.iter().any(|(k, v)| k == "bar" && v == "foo"),
        "bar → foo not found in {pairs:?}"
    );
}

#[test]
fn test_hash_pair_fizzfizz_buzzfizz() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    let pairs = hash_pairs(&rdb);
    assert!(
        pairs.iter().any(|(k, v)| k == "fizzfizz" && v == "buzzfizz"),
        "fizzfizz → buzzfizz not found in {pairs:?}"
    );
}

#[test]
fn test_hash_pair_test_value() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    let pairs = hash_pairs(&rdb);
    let (_, val) = pairs
        .iter()
        .find(|(k, _)| k == "test")
        .expect("key 'test' not found");
    assert_eq!(val, &format!("R{}", "E".repeat(46)));
}

#[test]
fn test_checksum_present() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert!(rdb.checksum.is_some(), "checksum should be present for version >= 5");
}

#[test]
fn test_checksum_value() {
    let data = load_dump();
    let rdb = RDB::new(&data).unwrap();
    assert_eq!(rdb.checksum, Some(12984812135656514652));
}

#[test]
fn test_empty_input_returns_error() {
    assert!(RDB::new(b"").is_err());
}

#[test]
fn test_truncated_after_magic_returns_error() {
    // Only 5 bytes — magic present but no version
    assert!(RDB::new(b"REDIS").is_err());
}

#[test]
fn test_wrong_magic_returns_error() {
    // Correct length and version bytes but wrong magic word
    assert!(RDB::new(b"WEDIS0011\xff").is_err());
}

#[test]
fn test_valid_magic_wrong_version_still_parses() {
    // A minimal RDB: magic + version "0001" + EOF opcode (no checksum below v5)
    let data = b"REDIS0001\xff";
    // Should parse without error even if we don't recognise all opcodes
    assert!(RDB::new(data).is_ok());
}
