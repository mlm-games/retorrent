use crate::error::{Result, TorrentError};
use std::collections::BTreeMap;
use std::str;

#[derive(Debug, Clone, PartialEq)]
pub enum BencodeValue {
    Integer(i64),
    ByteString(Vec<u8>),
    List(Vec<BencodeValue>),
    Dict(BTreeMap<String, BencodeValue>),
}

impl BencodeValue {
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            BencodeValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            BencodeValue::ByteString(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&str> {
        match self {
            BencodeValue::ByteString(b) => str::from_utf8(b).ok(),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[BencodeValue]> {
        match self {
            BencodeValue::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<String, BencodeValue>> {
        match self {
            BencodeValue::Dict(d) => Some(d),
            _ => None,
        }
    }

    pub fn dict_get(&self, key: &str) -> Option<&BencodeValue> {
        self.as_dict()?.get(key)
    }
}

const MAX_PARSE_DEPTH: usize = 128;

pub struct BencodeParser;

impl BencodeParser {
    pub fn parse(data: &[u8]) -> Result<BencodeValue> {
        let (value, _) = Self::parse_value(data, 0, 0)?;
        Ok(value)
    }

    pub fn encode(value: &BencodeValue) -> Vec<u8> {
        let mut buf = Vec::new();
        Self::encode_value(value, &mut buf);
        buf
    }

    fn parse_value(data: &[u8], pos: usize, depth: usize) -> Result<(BencodeValue, usize)> {
        if depth > MAX_PARSE_DEPTH {
            return Err(TorrentError::BencodeParse(
                "Max parse depth exceeded".to_string(),
            ));
        }
        if pos >= data.len() {
            return Err(TorrentError::BencodeParse(
                "Unexpected end of data".to_string(),
            ));
        }

        match data[pos] {
            b'i' => Self::parse_integer(data, pos),
            b'l' => Self::parse_list(data, pos, depth + 1),
            b'd' => Self::parse_dict(data, pos, depth + 1),
            b'0'..=b'9' => Self::parse_byte_string(data, pos),
            c => Err(TorrentError::BencodeParse(format!(
                "Unexpected byte: {} at pos {}",
                c, pos
            ))),
        }
    }

    fn parse_integer(data: &[u8], pos: usize) -> Result<(BencodeValue, usize)> {
        let end = data[pos + 1..]
            .iter()
            .position(|&b| b == b'e')
            .ok_or_else(|| TorrentError::BencodeParse("Missing 'e' for integer".to_string()))?
            + pos
            + 1;

        let num_str = str::from_utf8(&data[pos + 1..end])
            .map_err(|e| TorrentError::BencodeParse(e.to_string()))?;

        if num_str == "-0"
            || (num_str.len() > 1
                && num_str.starts_with('0'))
            || (num_str.len() > 2
                && num_str.starts_with("-0"))
        {
            return Err(TorrentError::BencodeParse(format!(
                "Invalid integer format: {}",
                num_str
            )));
        }

        let num: i64 = num_str
            .parse()
            .map_err(|e: std::num::ParseIntError| TorrentError::BencodeParse(e.to_string()))?;

        Ok((BencodeValue::Integer(num), end + 1))
    }

    fn parse_byte_string(data: &[u8], pos: usize) -> Result<(BencodeValue, usize)> {
        let colon = data[pos..]
            .iter()
            .position(|&b| b == b':')
            .ok_or_else(|| TorrentError::BencodeParse("Missing ':' for string".to_string()))?
            + pos;

        let len_str = str::from_utf8(&data[pos..colon])
            .map_err(|e| TorrentError::BencodeParse(e.to_string()))?;

        let len: usize = len_str
            .parse()
            .map_err(|e: std::num::ParseIntError| TorrentError::BencodeParse(e.to_string()))?;

        let start = colon + 1;
        let end = start + len;

        if end > data.len() {
            return Err(TorrentError::BencodeParse(
                "String extends past end of data".to_string(),
            ));
        }

        Ok((BencodeValue::ByteString(data[start..end].to_vec()), end))
    }

    fn parse_list(data: &[u8], pos: usize, depth: usize) -> Result<(BencodeValue, usize)> {
        let mut items = Vec::new();
        let mut current = pos + 1;

        while current < data.len() && data[current] != b'e' {
            let (value, next) = Self::parse_value(data, current, depth)?;
            items.push(value);
            current = next;
        }

        if current >= data.len() {
            return Err(TorrentError::BencodeParse(
                "Missing 'e' for list".to_string(),
            ));
        }

        Ok((BencodeValue::List(items), current + 1))
    }

    fn parse_dict(data: &[u8], pos: usize, depth: usize) -> Result<(BencodeValue, usize)> {
        let mut map = BTreeMap::new();
        let mut current = pos + 1;

        while current < data.len() && data[current] != b'e' {
            let (key_val, next) = Self::parse_byte_string(data, current)?;
            let key = match key_val {
                BencodeValue::ByteString(ref b) => String::from_utf8_lossy(b).to_string(),
                _ => {
                    return Err(TorrentError::BencodeParse(
                        "Dict key must be string".to_string(),
                    ))
                }
            };

            let (value, next2) = Self::parse_value(data, next, depth)?;
            map.insert(key, value);
            current = next2;
        }

        if current >= data.len() {
            return Err(TorrentError::BencodeParse(
                "Missing 'e' for dict".to_string(),
            ));
        }

        Ok((BencodeValue::Dict(map), current + 1))
    }

    fn encode_value(value: &BencodeValue, buf: &mut Vec<u8>) {
        match value {
            BencodeValue::Integer(i) => {
                buf.push(b'i');
                buf.extend_from_slice(i.to_string().as_bytes());
                buf.push(b'e');
            }
            BencodeValue::ByteString(s) => {
                buf.extend_from_slice(s.len().to_string().as_bytes());
                buf.push(b':');
                buf.extend_from_slice(s);
            }
            BencodeValue::List(l) => {
                buf.push(b'l');
                for item in l {
                    Self::encode_value(item, buf);
                }
                buf.push(b'e');
            }
            BencodeValue::Dict(d) => {
                buf.push(b'd');
                for (key, val) in d {
                    buf.extend_from_slice(key.len().to_string().as_bytes());
                    buf.push(b':');
                    buf.extend_from_slice(key.as_bytes());
                    Self::encode_value(val, buf);
                }
                buf.push(b'e');
            }
        }
    }
}

pub fn extract_info_raw(data: &[u8]) -> Result<&[u8]> {
    if data.is_empty() || data[0] != b'd' {
        return Err(TorrentError::BencodeParse("Root not a dict".to_string()));
    }
    // Walk raw bytes of root dict to find exact position of the "info" value.
    // This avoids searching for re-encoded bytes which can match at the wrong location.
    let mut pos = 1; // skip 'd'
    while pos < data.len() && data[pos] != b'e' {
        let (key_val, next) = BencodeParser::parse_byte_string(data, pos)?;
        let key = match key_val {
            BencodeValue::ByteString(ref b) => b.as_slice(),
            _ => unreachable!(),
        };
        let (_, val_end) = BencodeParser::parse_value(data, next, 0)?;
        if key == b"info" {
            return Ok(&data[next..val_end]);
        }
        pos = val_end;
    }
    Err(TorrentError::BencodeParse("No info dict found".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_integer() {
        let val = BencodeParser::parse(b"i42e").unwrap();
        assert_eq!(val.as_integer(), Some(42));
    }

    #[test]
    fn test_parse_string() {
        let val = BencodeParser::parse(b"4:spam").unwrap();
        assert_eq!(val.as_string(), Some("spam"));
    }

    #[test]
    fn test_parse_list() {
        let val = BencodeParser::parse(b"l4:spami42ee").unwrap();
        let list = val.as_list().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_roundtrip() {
        let original = BencodeValue::Dict({
            let mut m = BTreeMap::new();
            m.insert("key".to_string(), BencodeValue::Integer(123));
            m
        });
        let encoded = BencodeParser::encode(&original);
        let decoded = BencodeParser::parse(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_parse_synthetic_torrent() {
        // Build a minimal valid .torrent bencode
        let info_dict = BencodeValue::Dict({
            let mut m = std::collections::BTreeMap::new();
            m.insert("name".into(), BencodeValue::ByteString(b"testfile".to_vec()));
            m.insert("piece length".into(), BencodeValue::Integer(16384));
            m.insert("length".into(), BencodeValue::Integer(16384));
            m.insert("pieces".into(), BencodeValue::ByteString(vec![0u8; 20]));
            m
        });
        let info_encoded = BencodeParser::encode(&info_dict);
        let root = BencodeValue::Dict({
            let mut m = std::collections::BTreeMap::new();
            m.insert("announce".into(), BencodeValue::ByteString(b"http://tracker.test/announce".to_vec()));
            m.insert("info".into(), BencodeValue::ByteString(info_encoded.clone()));
            m
        });
        let _data = BencodeParser::encode(&root);

        // Now embed it as if it were the raw info dict inside a real torrent
        // Re-encode with proper structure
        let root2 = BencodeValue::Dict({
            let mut m = std::collections::BTreeMap::new();
            m.insert("announce".into(), BencodeValue::ByteString(b"http://tracker.test/announce".to_vec()));
            m.insert("info".into(), info_dict);
            m
        });
        let torrent_data = BencodeParser::encode(&root2);

        let parsed = BencodeParser::parse(&torrent_data).expect("Should parse");
        let dict = parsed.as_dict().expect("Root should be a dict");
        assert!(dict.contains_key("info"));
        assert!(dict.contains_key("announce"));
    }

    #[test]
    fn test_extract_info_raw_synthetic() {
        // Build a minimal torrent with known info dict
        let info_dict = BencodeValue::Dict({
            let mut m = std::collections::BTreeMap::new();
            m.insert("name".into(), BencodeValue::ByteString(b"test".to_vec()));
            m.insert("piece length".into(), BencodeValue::Integer(16384));
            m.insert("length".into(), BencodeValue::Integer(16384));
            m.insert("pieces".into(), BencodeValue::ByteString(vec![0u8; 20]));
            m
        });
        let info_encoded = BencodeParser::encode(&info_dict);
        let root = BencodeValue::Dict({
            let mut m = std::collections::BTreeMap::new();
            m.insert("announce".into(), BencodeValue::ByteString(b"http://t.test/a".to_vec()));
            m.insert("info".into(), info_dict);
            m
        });
        let data = BencodeParser::encode(&root);

        let info_raw = extract_info_raw(&data).expect("Should find info dict");
        assert_eq!(&info_raw[..], &info_encoded[..]);
    }
}

#[cfg(test)]
mod metainfo_tests {
    use crate::bencode::{BencodeParser, BencodeValue};
    use crate::metainfo::MetaInfo;
    use sha1::{Digest, Sha1};
    use std::collections::BTreeMap;

    fn make_test_torrent_bytes() -> Vec<u8> {
        let info_dict = BencodeValue::Dict({
            let mut m = BTreeMap::new();
            m.insert("name".into(), BencodeValue::ByteString(b"testfile".to_vec()));
            m.insert("piece length".into(), BencodeValue::Integer(16384));
            m.insert("length".into(), BencodeValue::Integer(32768));
            m.insert("pieces".into(), BencodeValue::ByteString(vec![0u8; 40])); // 2 pieces
            m
        });
        let root = BencodeValue::Dict({
            let mut m = BTreeMap::new();
            m.insert("announce".into(), BencodeValue::ByteString(b"http://tracker.test/announce".to_vec()));
            m.insert("comment".into(), BencodeValue::ByteString(b"test comment".to_vec()));
            m.insert("created by".into(), BencodeValue::ByteString(b"test".to_vec()));
            m.insert("creation date".into(), BencodeValue::Integer(1234567890));
            m.insert("info".into(), info_dict);
            m
        });
        BencodeParser::encode(&root)
    }

    #[test]
    fn test_parse_metainfo() {
        let data = make_test_torrent_bytes();
        let meta = MetaInfo::from_bytes(&data).expect("Should parse metainfo");

        assert_eq!(meta.name, "testfile");
        assert!(meta.total_size > 0);
        assert_eq!(meta.pieces.len(), 2);
        assert_eq!(meta.piece_length, 16384);
        assert_eq!(meta.files.len(), 1);
        assert_eq!(meta.announce.as_deref(), Some("http://tracker.test/announce"));
        assert_eq!(meta.comment.as_deref(), Some("test comment"));

        // Verify info hash
        let mut hasher = Sha1::new();
        let info_raw = crate::bencode::extract_info_raw(&data).unwrap();
        hasher.update(info_raw);
        let expected_hash = hasher.finalize();
        assert_eq!(&meta.info_hash.0, expected_hash.as_slice());
    }

    #[test]
    fn test_metainfo_piece_count() {
        let data = make_test_torrent_bytes();
        let meta = MetaInfo::from_bytes(&data).expect("Should parse metainfo");

        let num_pieces = meta.num_pieces();
        assert_eq!(num_pieces, 2);
        assert_eq!(num_pieces as usize, meta.pieces.len());

        let mut calculated_size = 0u64;
        for i in 0..num_pieces {
            calculated_size += meta.piece_size(i);
        }
        assert_eq!(calculated_size, meta.total_size);
    }
}
