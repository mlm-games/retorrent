use std::collections::BTreeMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use crate::bencode::{BencodeParser, BencodeValue};
use crate::types::InfoHash;

pub type TransactionId = u16;

pub fn encode_transaction_id(tid: u16) -> Vec<u8> {
    vec![(tid >> 8) as u8, (tid & 0xff) as u8]
}

pub fn decode_transaction_id(data: &[u8]) -> Option<u16> {
    if data.len() != 2 {
        return None;
    }
    Some((data[0] as u16) << 8 | data[1] as u16)
}

#[derive(Debug, Clone)]
pub enum QueryMethod {
    Ping,
    FindNode,
    GetPeers,
    AnnouncePeer,
}

impl QueryMethod {
    fn from_bytes(b: &[u8]) -> Option<Self> {
        match b {
            b"ping" => Some(Self::Ping),
            b"find_node" => Some(Self::FindNode),
            b"get_peers" => Some(Self::GetPeers),
            b"announce_peer" => Some(Self::AnnouncePeer),
            _ => None,
        }
    }

    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Ping => b"ping",
            Self::FindNode => b"find_node",
            Self::GetPeers => b"get_peers",
            Self::AnnouncePeer => b"announce_peer",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Want {
    V4,
    V6,
    Both,
    None,
}

impl Want {
    pub fn for_addr(addr: SocketAddr) -> Self {
        if addr.is_ipv6() { Self::V6 } else { Self::V4 }
    }

    pub fn serialize_list(self) -> BencodeValue {
        let items: Vec<BencodeValue> = match self {
            Self::V4 => vec![bencode_str(b"n4")],
            Self::V6 => vec![bencode_str(b"n6")],
            Self::Both => vec![bencode_str(b"n4"), bencode_str(b"n6")],
            Self::None => Vec::new(),
        };
        BencodeValue::List(items)
    }

    fn from_list(list: &[BencodeValue]) -> Self {
        let mut v4 = false;
        let mut v6 = false;
        for item in list {
            if let Some(b) = item.as_bytes() {
                match b {
                    b"n4" => v4 = true,
                    b"n6" => v6 = true,
                    _ => {}
                }
            }
        }
        match (v4, v6) {
            (true, true) => Self::Both,
            (true, false) => Self::V4,
            (false, true) => Self::V6,
            (false, false) => Self::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ErrorDescription {
    pub code: i32,
    pub message: Vec<u8>,
}

fn build_compact_node_info_v4(id: InfoHash, addr: SocketAddrV4) -> Vec<u8> {
    let mut buf = Vec::with_capacity(26);
    buf.extend_from_slice(&id.0);
    buf.extend_from_slice(&addr.ip().octets());
    buf.extend_from_slice(&addr.port().to_be_bytes());
    buf
}

fn parse_compact_node_info_v4(data: &[u8]) -> Option<(InfoHash, SocketAddrV4)> {
    if data.len() < 26 {
        return None;
    }
    let id = InfoHash::from_bytes(&data[..20])?;
    let ip = Ipv4Addr::new(data[20], data[21], data[22], data[23]);
    let port = u16::from_be_bytes([data[24], data[25]]);
    Some((id, SocketAddrV4::new(ip, port)))
}

fn build_compact_node_info_v6(id: InfoHash, addr: SocketAddrV6) -> Vec<u8> {
    let mut buf = Vec::with_capacity(38);
    buf.extend_from_slice(&id.0);
    buf.extend_from_slice(&addr.ip().octets());
    buf.extend_from_slice(&addr.port().to_be_bytes());
    buf
}

fn parse_compact_node_info_v6(data: &[u8]) -> Option<(InfoHash, SocketAddrV6)> {
    if data.len() < 38 {
        return None;
    }
    let id = InfoHash::from_bytes(&data[..20])?;
    let ip_bytes = &data[20..36];
    let octets: [u8; 16] = ip_bytes.try_into().ok()?;
    let ip = Ipv6Addr::from(octets);
    let port = u16::from_be_bytes([data[36], data[37]]);
    Some((id, SocketAddrV6::new(ip, port, 0, 0)))
}

pub fn build_compact_peer_addr(addr: SocketAddr) -> Vec<u8> {
    match addr {
        SocketAddr::V4(v4) => {
            let mut buf = vec![0u8; 6];
            buf[..4].copy_from_slice(&v4.ip().octets());
            buf[4..6].copy_from_slice(&v4.port().to_be_bytes());
            buf
        }
        SocketAddr::V6(v6) => {
            let mut buf = vec![0u8; 18];
            buf[..16].copy_from_slice(&v6.ip().octets());
            buf[16..18].copy_from_slice(&v6.port().to_be_bytes());
            buf
        }
    }
}

pub fn parse_compact_peer_addr(data: &[u8]) -> Option<SocketAddr> {
    match data.len() {
        6 => {
            let ip = Ipv4Addr::new(data[0], data[1], data[2], data[3]);
            let port = u16::from_be_bytes([data[4], data[5]]);
            Some(SocketAddr::V4(SocketAddrV4::new(ip, port)))
        }
        18 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&data[..16]);
            let ip = Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([data[16], data[17]]);
            Some(SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0)))
        }
        _ => None,
    }
}

pub fn parse_compact_nodes(data: &[u8]) -> Vec<(InfoHash, SocketAddr)> {
    let mut nodes = Vec::new();
    let mut offset = 0;
    while offset + 26 <= data.len() {
        if let Some((id, v4)) = parse_compact_node_info_v4(&data[offset..]) {
            nodes.push((id, SocketAddr::V4(v4)));
        }
        offset += 26;
    }
    nodes
}

pub fn parse_compact_nodes_v6(data: &[u8]) -> Vec<(InfoHash, SocketAddr)> {
    let mut nodes = Vec::new();
    let mut offset = 0;
    while offset + 38 <= data.len() {
        if let Some((id, v6)) = parse_compact_node_info_v6(&data[offset..]) {
            nodes.push((id, SocketAddr::V6(v6)));
        }
        offset += 38;
    }
    nodes
}

fn bencode_str(s: &[u8]) -> BencodeValue {
    BencodeValue::ByteString(s.to_vec())
}

fn bencode_int(i: i64) -> BencodeValue {
    BencodeValue::Integer(i)
}

fn bencode_id20(id: InfoHash) -> BencodeValue {
    BencodeValue::ByteString(id.0.to_vec())
}

#[derive(Debug, Clone)]
pub enum Message {
    PingRequest {
        transaction_id: Vec<u8>,
        id: InfoHash,
    },
    PingResponse {
        transaction_id: Vec<u8>,
        id: InfoHash,
        ip: Option<SocketAddr>,
    },
    FindNodeRequest {
        transaction_id: Vec<u8>,
        id: InfoHash,
        target: InfoHash,
        want: Want,
    },
    FindNodeResponse {
        transaction_id: Vec<u8>,
        id: InfoHash,
        nodes: Vec<u8>,
        nodes6: Vec<u8>,
        ip: Option<SocketAddr>,
    },
    GetPeersRequest {
        transaction_id: Vec<u8>,
        id: InfoHash,
        info_hash: InfoHash,
        want: Want,
    },
    GetPeersResponse {
        transaction_id: Vec<u8>,
        id: InfoHash,
        token: Vec<u8>,
        values: Vec<SocketAddr>,
        nodes: Vec<u8>,
        nodes6: Vec<u8>,
        ip: Option<SocketAddr>,
    },
    AnnouncePeerRequest {
        transaction_id: Vec<u8>,
        id: InfoHash,
        info_hash: InfoHash,
        token: Vec<u8>,
        port: u16,
        implied_port: u8,
    },
    AnnouncePeerResponse {
        transaction_id: Vec<u8>,
        id: InfoHash,
        ip: Option<SocketAddr>,
    },
    Error {
        transaction_id: Vec<u8>,
        code: i32,
        message: Vec<u8>,
    },
}

impl Message {
    pub fn transaction_id(&self) -> &[u8] {
        match self {
            Message::PingRequest { transaction_id, .. } => transaction_id,
            Message::PingResponse { transaction_id, .. } => transaction_id,
            Message::FindNodeRequest { transaction_id, .. } => transaction_id,
            Message::FindNodeResponse { transaction_id, .. } => transaction_id,
            Message::GetPeersRequest { transaction_id, .. } => transaction_id,
            Message::GetPeersResponse { transaction_id, .. } => transaction_id,
            Message::AnnouncePeerRequest { transaction_id, .. } => transaction_id,
            Message::AnnouncePeerResponse { transaction_id, .. } => transaction_id,
            Message::Error { transaction_id, .. } => transaction_id,
        }
    }
}

pub fn serialize(message: &Message) -> Vec<u8> {
    let dict = match message {
        Message::PingRequest { transaction_id, id } => BencodeValue::Dict(BTreeMap::from([
            (String::from("t"), bencode_str(transaction_id)),
            (String::from("y"), bencode_str(b"q")),
            (String::from("q"), bencode_str(b"ping")),
            (
                String::from("a"),
                BencodeValue::Dict(BTreeMap::from([(String::from("id"), bencode_id20(*id))])),
            ),
        ])),

        Message::PingResponse {
            transaction_id,
            id,
            ip,
        } => {
            let mut r = BTreeMap::new();
            r.insert(String::from("id"), bencode_id20(*id));
            let mut d = BTreeMap::new();
            d.insert(String::from("t"), bencode_str(transaction_id));
            d.insert(String::from("y"), bencode_str(b"r"));
            d.insert(String::from("r"), BencodeValue::Dict(r));
            if let Some(addr) = ip {
                let compact = build_compact_peer_addr(*addr);
                d.insert(String::from("ip"), bencode_str(&compact));
            }
            BencodeValue::Dict(d)
        }

        Message::FindNodeRequest {
            transaction_id,
            id,
            target,
            want,
        } => {
            let mut a = BTreeMap::new();
            a.insert(String::from("id"), bencode_id20(*id));
            a.insert(String::from("target"), bencode_id20(*target));
            if *want != Want::None {
                a.insert(String::from("want"), want.serialize_list());
            }
            BencodeValue::Dict(BTreeMap::from([
                (String::from("t"), bencode_str(transaction_id)),
                (String::from("y"), bencode_str(b"q")),
                (String::from("q"), bencode_str(b"find_node")),
                (String::from("a"), BencodeValue::Dict(a)),
            ]))
        }

        Message::FindNodeResponse {
            transaction_id,
            id,
            nodes,
            nodes6,
            ip,
        } => {
            let mut r = BTreeMap::new();
            r.insert(String::from("id"), bencode_id20(*id));
            if !nodes.is_empty() {
                r.insert(String::from("nodes"), bencode_str(nodes));
            }
            if !nodes6.is_empty() {
                r.insert(String::from("nodes6"), bencode_str(nodes6));
            }
            let mut d = BTreeMap::new();
            d.insert(String::from("t"), bencode_str(transaction_id));
            d.insert(String::from("y"), bencode_str(b"r"));
            d.insert(String::from("r"), BencodeValue::Dict(r));
            if let Some(addr) = ip {
                d.insert(
                    String::from("ip"),
                    bencode_str(&build_compact_peer_addr(*addr)),
                );
            }
            BencodeValue::Dict(d)
        }

        Message::GetPeersRequest {
            transaction_id,
            id,
            info_hash,
            want,
        } => {
            let mut a = BTreeMap::new();
            a.insert(String::from("id"), bencode_id20(*id));
            a.insert(String::from("info_hash"), bencode_id20(*info_hash));
            if *want != Want::None {
                a.insert(String::from("want"), want.serialize_list());
            }
            BencodeValue::Dict(BTreeMap::from([
                (String::from("t"), bencode_str(transaction_id)),
                (String::from("y"), bencode_str(b"q")),
                (String::from("q"), bencode_str(b"get_peers")),
                (String::from("a"), BencodeValue::Dict(a)),
            ]))
        }

        Message::GetPeersResponse {
            transaction_id,
            id,
            token,
            values,
            nodes,
            nodes6,
            ip,
        } => {
            let mut r = BTreeMap::new();
            r.insert(String::from("id"), bencode_id20(*id));
            r.insert(String::from("token"), bencode_str(token));
            if !values.is_empty() {
                let list: Vec<BencodeValue> = values
                    .iter()
                    .map(|a| bencode_str(&build_compact_peer_addr(*a)))
                    .collect();
                r.insert(String::from("values"), BencodeValue::List(list));
            }
            if !nodes.is_empty() {
                r.insert(String::from("nodes"), bencode_str(nodes));
            }
            if !nodes6.is_empty() {
                r.insert(String::from("nodes6"), bencode_str(nodes6));
            }
            let mut d = BTreeMap::new();
            d.insert(String::from("t"), bencode_str(transaction_id));
            d.insert(String::from("y"), bencode_str(b"r"));
            d.insert(String::from("r"), BencodeValue::Dict(r));
            if let Some(addr) = ip {
                d.insert(
                    String::from("ip"),
                    bencode_str(&build_compact_peer_addr(*addr)),
                );
            }
            BencodeValue::Dict(d)
        }

        Message::AnnouncePeerRequest {
            transaction_id,
            id,
            info_hash,
            token,
            port,
            implied_port,
        } => BencodeValue::Dict(BTreeMap::from([
            (String::from("t"), bencode_str(transaction_id)),
            (String::from("y"), bencode_str(b"q")),
            (String::from("q"), bencode_str(b"announce_peer")),
            (
                String::from("a"),
                BencodeValue::Dict(BTreeMap::from([
                    (String::from("id"), bencode_id20(*id)),
                    (String::from("info_hash"), bencode_id20(*info_hash)),
                    (String::from("token"), bencode_str(token)),
                    (String::from("port"), bencode_int(*port as i64)),
                    (
                        String::from("implied_port"),
                        bencode_int(*implied_port as i64),
                    ),
                ])),
            ),
        ])),

        Message::AnnouncePeerResponse {
            transaction_id,
            id,
            ip,
        } => {
            let mut r = BTreeMap::new();
            r.insert(String::from("id"), bencode_id20(*id));
            let mut d = BTreeMap::new();
            d.insert(String::from("t"), bencode_str(transaction_id));
            d.insert(String::from("y"), bencode_str(b"r"));
            d.insert(String::from("r"), BencodeValue::Dict(r));
            if let Some(addr) = ip {
                d.insert(
                    String::from("ip"),
                    bencode_str(&build_compact_peer_addr(*addr)),
                );
            }
            BencodeValue::Dict(d)
        }

        Message::Error {
            transaction_id,
            code,
            message,
        } => BencodeValue::Dict(BTreeMap::from([
            (String::from("t"), bencode_str(transaction_id)),
            (String::from("y"), bencode_str(b"e")),
            (
                String::from("e"),
                BencodeValue::List(vec![bencode_int(*code as i64), bencode_str(message)]),
            ),
        ])),
    };
    BencodeParser::encode(&dict)
}

pub fn deserialize(data: &[u8]) -> Option<Message> {
    let value = BencodeParser::parse(data).ok()?;
    let dict = value.as_dict()?;

    let y = dict.get("y")?.as_bytes()?;
    let t = dict.get("t")?.as_bytes()?.to_vec();

    match y {
        b"q" => {
            let q = dict.get("q")?.as_bytes()?;
            let a = dict.get("a")?.as_dict()?;
            let id = parse_id20(a.get("id")?.as_bytes()?)?;
            match q {
                b"ping" => Some(Message::PingRequest {
                    transaction_id: t,
                    id,
                }),
                b"find_node" => {
                    let target = parse_id20(a.get("target")?.as_bytes()?)?;
                    let want = a
                        .get("want")
                        .and_then(|v| v.as_list())
                        .map(Want::from_list)
                        .unwrap_or(Want::None);
                    Some(Message::FindNodeRequest {
                        transaction_id: t,
                        id,
                        target,
                        want,
                    })
                }
                b"get_peers" => {
                    let info_hash = parse_id20(a.get("info_hash")?.as_bytes()?)?;
                    let want = a
                        .get("want")
                        .and_then(|v| v.as_list())
                        .map(Want::from_list)
                        .unwrap_or(Want::None);
                    Some(Message::GetPeersRequest {
                        transaction_id: t,
                        id,
                        info_hash,
                        want,
                    })
                }
                b"announce_peer" => {
                    let info_hash = parse_id20(a.get("info_hash")?.as_bytes()?)?;
                    let token = a.get("token")?.as_bytes()?.to_vec();
                    let port = a.get("port")?.as_integer().unwrap_or(0) as u16;
                    let implied_port = a.get("implied_port")?.as_integer().unwrap_or(0) as u8;
                    Some(Message::AnnouncePeerRequest {
                        transaction_id: t,
                        id,
                        info_hash,
                        token,
                        port,
                        implied_port,
                    })
                }
                _ => None,
            }
        }
        b"r" => {
            let r = dict.get("r")?.as_dict()?;
            let id = parse_id20(r.get("id")?.as_bytes()?)?;
            let ip = dict
                .get("ip")
                .and_then(|v| parse_compact_peer_addr(v.as_bytes()?));

            let has_token = r.contains_key("token");
            let has_values = r.contains_key("values");
            let has_nodes = r.contains_key("nodes");

            if has_token || has_values {
                let token = r
                    .get("token")
                    .map(|v| v.as_bytes().unwrap_or_default().to_vec())
                    .unwrap_or_default();
                let values = r
                    .get("values")
                    .map(|v| {
                        v.as_list()
                            .map(|list| {
                                list.iter()
                                    .filter_map(|item| item.as_bytes())
                                    .filter_map(parse_compact_peer_addr)
                                    .collect::<Vec<SocketAddr>>()
                            })
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
                let nodes = r
                    .get("nodes")
                    .and_then(|v| v.as_bytes())
                    .unwrap_or_default()
                    .to_vec();
                let nodes6 = r
                    .get("nodes6")
                    .and_then(|v| v.as_bytes())
                    .unwrap_or_default()
                    .to_vec();
                Some(Message::GetPeersResponse {
                    transaction_id: t,
                    id,
                    token,
                    values,
                    nodes,
                    nodes6,
                    ip,
                })
            } else if has_nodes {
                let nodes = r
                    .get("nodes")
                    .and_then(|v| v.as_bytes())
                    .unwrap_or_default()
                    .to_vec();
                let nodes6 = r
                    .get("nodes6")
                    .and_then(|v| v.as_bytes())
                    .unwrap_or_default()
                    .to_vec();
                Some(Message::FindNodeResponse {
                    transaction_id: t,
                    id,
                    nodes,
                    nodes6,
                    ip,
                })
            } else {
                Some(Message::PingResponse {
                    transaction_id: t,
                    id,
                    ip,
                })
            }
        }
        b"e" => {
            let e = dict.get("e")?.as_list()?;
            let code = e.first().and_then(|v| v.as_integer()).unwrap_or(0) as i32;
            let msg = e.get(1).and_then(|v| v.as_bytes()).unwrap_or(b"").to_vec();
            Some(Message::Error {
                transaction_id: t,
                code,
                message: msg,
            })
        }
        _ => None,
    }
}

fn parse_id20(data: &[u8]) -> Option<InfoHash> {
    if data.len() != 20 {
        return None;
    }
    let mut arr = [0u8; 20];
    arr.copy_from_slice(data);
    Some(InfoHash(arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn test_id(byte: u8) -> InfoHash {
        InfoHash([byte; 20])
    }

    #[test]
    fn ping_request_roundtrip() {
        let id = test_id(0xaa);
        let msg = Message::PingRequest {
            transaction_id: vec![0x12, 0x34],
            id,
        };
        let bytes = serialize(&msg);
        let parsed = deserialize(&bytes).expect("deserialize");
        match parsed {
            Message::PingRequest {
                transaction_id,
                id: pid,
            } => {
                assert_eq!(transaction_id, vec![0x12, 0x34]);
                assert_eq!(pid, id);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn find_node_request_want_roundtrip() {
        let id = test_id(0xbb);
        let target = test_id(0xcc);
        let msg = Message::FindNodeRequest {
            transaction_id: b"aa".to_vec(),
            id,
            target,
            want: Want::V4,
        };
        let bytes = serialize(&msg);
        let parsed = deserialize(&bytes).expect("deserialize");
        match parsed {
            Message::FindNodeRequest {
                id: pid,
                target: pt,
                want,
                ..
            } => {
                assert_eq!(pid, id);
                assert_eq!(pt, target);
                assert_eq!(want, Want::V4);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn get_peers_response_values_as_list_roundtrip() {
        let id = test_id(0xdd);
        let v4 = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881));
        let v4b = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(5, 6, 7, 8), 51413));
        let values = vec![v4, v4b];
        let msg = Message::GetPeersResponse {
            transaction_id: b"tt".to_vec(),
            id,
            token: b"abcde".to_vec(),
            values: values.clone(),
            nodes: Vec::new(),
            nodes6: Vec::new(),
            ip: None,
        };
        let bytes = serialize(&msg);
        let parsed_val = BencodeParser::parse(&bytes).expect("parse");
        let r = parsed_val
            .as_dict()
            .expect("dict")
            .get("r")
            .expect("r")
            .as_dict()
            .expect("r dict");
        let values_val = r.get("values").expect("values field");
        assert!(
            matches!(values_val, BencodeValue::List(_)),
            "values should be a bencode list, got {:?}",
            values_val
        );
        let parsed = deserialize(&bytes).expect("deserialize");
        match parsed {
            Message::GetPeersResponse {
                values: pv, token, ..
            } => {
                assert_eq!(pv, values);
                assert_eq!(token, b"abcde");
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn announce_peer_wires_format() {
        let id = test_id(0x01);
        let info_hash = test_id(0x02);
        let msg = Message::AnnouncePeerRequest {
            transaction_id: b"x1".to_vec(),
            id,
            info_hash,
            token: b"tkn".to_vec(),
            port: 6881,
            implied_port: 0,
        };
        let bytes = serialize(&msg);
        let expected = b"d1:ad2:id20:".to_vec();
        assert_eq!(&bytes[..expected.len()], &expected[..]);
        let parsed = deserialize(&bytes).expect("deserialize");
        match parsed {
            Message::AnnouncePeerRequest {
                port, implied_port, ..
            } => {
                assert_eq!(port, 6881);
                assert_eq!(implied_port, 0);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn find_node_response_with_nodes6_roundtrip() {
        let id = test_id(0xee);
        let node_id = test_id(0xff);
        let mut nodes = Vec::new();
        nodes.extend_from_slice(&node_id.0);
        nodes.extend_from_slice(&[127, 0, 0, 1]);
        nodes.extend_from_slice(&6881u16.to_be_bytes());
        let mut nodes6 = Vec::new();
        let v6_id = test_id(0x77);
        nodes6.extend_from_slice(&v6_id.0);
        nodes6.extend_from_slice(&[
            0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x01,
        ]);
        nodes6.extend_from_slice(&51413u16.to_be_bytes());
        let msg = Message::FindNodeResponse {
            transaction_id: b"q2".to_vec(),
            id,
            nodes: nodes.clone(),
            nodes6: nodes6.clone(),
            ip: None,
        };
        let bytes = serialize(&msg);
        let parsed = deserialize(&bytes).expect("deserialize");
        match parsed {
            Message::FindNodeResponse {
                nodes: pn,
                nodes6: pn6,
                ..
            } => {
                assert_eq!(pn, nodes);
                assert_eq!(pn6, nodes6);
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn want_serialize_list_format() {
        let l = Want::Both.serialize_list();
        let bytes = BencodeParser::encode(&l);
        assert_eq!(bytes, b"l2:n42:n6e");
        let l = Want::V4.serialize_list();
        let bytes = BencodeParser::encode(&l);
        assert_eq!(bytes, b"l2:n4e");
        let l = Want::V6.serialize_list();
        let bytes = BencodeParser::encode(&l);
        assert_eq!(bytes, b"l2:n6e");
        let l = Want::None.serialize_list();
        let bytes = BencodeParser::encode(&l);
        assert_eq!(bytes, b"le");
    }
}
