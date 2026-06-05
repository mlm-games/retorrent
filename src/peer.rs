use crate::error::{Result, TorrentError};
use crate::types::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::net::TcpStream;

/// Tracks the state of an in-progress ut_metadata exchange.
pub struct MetadataState {
    /// Received data for each piece (None = not yet received).
    pub pieces: Vec<Option<Vec<u8>>>,
    /// Total size of the info-dict, learned from the first data message.
    pub total_size: usize,
    /// Number of pieces = total_size.div_ceil(16 * 1024).
    pub num_pieces: usize,
    /// The info-hash to verify the assembled metadata against.
    pub info_hash: InfoHash,
}

impl MetadataState {
    pub fn new(info_hash: InfoHash) -> Self {
        Self {
            pieces: Vec::new(),
            total_size: 0,
            num_pieces: 0,
            info_hash,
        }
    }

    /// Set the total size from the first data message and resize the piece buffer.
    pub fn set_total_size(&mut self, total_size: usize) {
        self.total_size = total_size;
        self.num_pieces = total_size.div_ceil(BLOCK_SIZE as usize);
        self.pieces = vec![None; self.num_pieces];
    }

    /// Store a received piece. Returns true if all pieces are now present.
    pub fn store_piece(&mut self, index: usize, data: Vec<u8>) -> bool {
        if index < self.pieces.len() {
            self.pieces[index] = Some(data);
        }
        self.pieces.iter().all(|p| p.is_some())
    }

    /// Assemble all received pieces into the raw info dict.
    pub fn assemble(&self) -> Option<Vec<u8>> {
        if !self.pieces.iter().all(|p| p.is_some()) {
            return None;
        }
        let mut result = Vec::with_capacity(self.total_size);
        for piece in &self.pieces {
            if let Some(data) = piece {
                result.extend_from_slice(data);
            }
        }
        result.truncate(self.total_size);
        Some(result)
    }

    /// Verify the assembled metadata against the info_hash.
    pub fn verify(&self, data: &[u8]) -> bool {
        use sha1::Digest;
        let mut hasher = sha1::Sha1::new();
        sha1::Digest::update(&mut hasher, data);
        let result = hasher.finalize();
        result.as_slice() == self.info_hash.as_bytes()
    }
}

#[derive(Debug)]
pub enum PeerMessage {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Vec<u8>),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        data: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    Extended {
        id: u8,
        payload: Vec<u8>,
    },
}

impl PeerMessage {
    /// Build the BEP-10 extended-handshake payload.
    /// Advertises `ut_pex` (id=1) and `ut_metadata` (id=2).
    pub fn build_extended_handshake_payload(reqq: u32) -> Vec<u8> {
        use crate::bencode::{BencodeParser, BencodeValue};
        use std::collections::BTreeMap;

        let mut m = BTreeMap::new();
        m.insert("ut_pex".to_string(), BencodeValue::Integer(1));
        m.insert("ut_metadata".to_string(), BencodeValue::Integer(2));
        let mut top = BTreeMap::new();
        top.insert("m".to_string(), BencodeValue::Dict(m));
        top.insert(
            "v".to_string(),
            BencodeValue::ByteString(b"retorrent-0.1.0".to_vec()),
        );
        top.insert("reqq".to_string(), BencodeValue::Integer(reqq as i64));
        BencodeParser::encode(&BencodeValue::Dict(top))
    }

    /// Parse the peer's extended handshake.
    /// Returns `(ut_pex_id, ut_metadata_id)` — both `None` when absent.
    pub fn parse_extended_handshake(payload: &[u8]) -> (Option<u8>, Option<u8>) {
        use crate::bencode::BencodeParser;
        let decoded = match BencodeParser::parse(payload) {
            Ok(v) => v,
            Err(_) => return (None, None),
        };
        let dict = match decoded.as_dict() {
            Some(d) => d,
            None => return (None, None),
        };
        let m = match dict.get("m").and_then(|v| v.as_dict()) {
            Some(m) => m,
            None => return (None, None),
        };
        let pex_id = m
            .get("ut_pex")
            .and_then(|v| v.as_integer())
            .map(|i| i as u8);
        let metadata_id = m
            .get("ut_metadata")
            .and_then(|v| v.as_integer())
            .map(|i| i as u8);
        (pex_id, metadata_id)
    }

    pub fn build_pex_payload(added: &[SocketAddrV4], dropped: &[SocketAddrV4]) -> Vec<u8> {
        use crate::bencode::{BencodeParser, BencodeValue};
        use std::collections::BTreeMap;

        let mut added_compact = Vec::with_capacity(added.len() * 6);
        for addr in added {
            added_compact.extend_from_slice(&addr.ip().octets());
            added_compact.extend_from_slice(&addr.port().to_be_bytes());
        }
        let mut dropped_compact = Vec::with_capacity(dropped.len() * 6);
        for addr in dropped {
            dropped_compact.extend_from_slice(&addr.ip().octets());
            dropped_compact.extend_from_slice(&addr.port().to_be_bytes());
        }

        let flags = vec![0x01u8; added.len()];

        let mut dict = BTreeMap::new();
        dict.insert("added".to_string(), BencodeValue::ByteString(added_compact));
        dict.insert("added.f".to_string(), BencodeValue::ByteString(flags));
        dict.insert(
            "dropped".to_string(),
            BencodeValue::ByteString(dropped_compact),
        );

        BencodeParser::encode(&BencodeValue::Dict(dict))
    }
}

impl PeerMessage {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            PeerMessage::KeepAlive => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 0).unwrap();
            }
            PeerMessage::Choke => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 1).unwrap();
                buf.push(0);
            }
            PeerMessage::Unchoke => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 1).unwrap();
                buf.push(1);
            }
            PeerMessage::Interested => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 1).unwrap();
                buf.push(2);
            }
            PeerMessage::NotInterested => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 1).unwrap();
                buf.push(3);
            }
            PeerMessage::Have(piece) => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 5).unwrap();
                buf.push(4);
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *piece).unwrap();
            }
            PeerMessage::Bitfield(bitfield) => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 1 + bitfield.len() as u32).unwrap();
                buf.push(5);
                buf.extend_from_slice(bitfield);
            }
            PeerMessage::Request {
                index,
                begin,
                length,
            } => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 13).unwrap();
                buf.push(6);
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *index).unwrap();
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *begin).unwrap();
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *length).unwrap();
            }
            PeerMessage::Piece { index, begin, data } => {
                buf.reserve(9 + data.len());
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 9 + data.len() as u32).unwrap();
                buf.push(7);
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *index).unwrap();
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *begin).unwrap();
                buf.extend_from_slice(data);
            }
            PeerMessage::Cancel {
                index,
                begin,
                length,
            } => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 13).unwrap();
                buf.push(8);
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *index).unwrap();
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *begin).unwrap();
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, *length).unwrap();
            }
            PeerMessage::Port(port) => {
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, 3).unwrap();
                buf.push(9);
                WriteBytesExt::write_u16::<BigEndian>(&mut buf, *port).unwrap();
            }
            PeerMessage::Extended { id, payload } => {
                let len = 2 + payload.len();
                WriteBytesExt::write_u32::<BigEndian>(&mut buf, len as u32).unwrap();
                buf.push(20);
                buf.push(*id);
                buf.extend_from_slice(payload);
            }
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Ok(PeerMessage::KeepAlive);
        }

        let id = data[0];
        let payload = &data[1..];
        let mut cursor = Cursor::new(payload);

        match id {
            0 => Ok(PeerMessage::Choke),
            1 => Ok(PeerMessage::Unchoke),
            2 => Ok(PeerMessage::Interested),
            3 => Ok(PeerMessage::NotInterested),
            4 => {
                let piece = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                Ok(PeerMessage::Have(piece))
            }
            5 => Ok(PeerMessage::Bitfield(payload.to_vec())),
            6 => {
                let index = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                let begin = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                let length = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                Ok(PeerMessage::Request {
                    index,
                    begin,
                    length,
                })
            }
            7 => {
                if payload.len() < 8 {
                    return Err(TorrentError::Peer(format!(
                        "Piece message too short: {} bytes",
                        payload.len()
                    )));
                }
                let index = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                let begin = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                Ok(PeerMessage::Piece {
                    index,
                    begin,
                    data: payload[8..].to_vec(),
                })
            }
            8 => {
                let index = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                let begin = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                let length = cursor
                    .read_u32::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                Ok(PeerMessage::Cancel {
                    index,
                    begin,
                    length,
                })
            }
            9 => {
                let port = cursor
                    .read_u16::<BigEndian>()
                    .map_err(|e| TorrentError::Peer(e.to_string()))?;
                Ok(PeerMessage::Port(port))
            }
            20 => {
                if payload.is_empty() {
                    return Ok(PeerMessage::Extended {
                        id: 0,
                        payload: vec![],
                    });
                }
                let ext_id = payload[0];
                Ok(PeerMessage::Extended {
                    id: ext_id,
                    payload: payload[1..].to_vec(),
                })
            }
            _ => Err(TorrentError::Peer(format!("Unknown message id: {}", id))),
        }
    }

    pub fn parse_pex_payload(payload: &[u8]) -> (Vec<SocketAddrV4>, Vec<SocketAddrV4>) {
        use crate::bencode::BencodeParser;

        let Ok(decoded) = BencodeParser::parse(payload) else {
            return (vec![], vec![]);
        };
        let dict = match decoded.as_dict() {
            Some(d) => d,
            None => return (vec![], vec![]),
        };

        let mut added = Vec::new();
        if let Some(added_bytes) = dict.get("added").and_then(|v| v.as_bytes()) {
            for chunk in added_bytes.chunks_exact(6) {
                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                if port > 0 {
                    added.push(SocketAddrV4::new(ip, port));
                }
            }
        }

        let mut dropped = Vec::new();
        if let Some(dropped_bytes) = dict.get("dropped").and_then(|v| v.as_bytes()) {
            for chunk in dropped_bytes.chunks_exact(6) {
                let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                if port > 0 {
                    dropped.push(SocketAddrV4::new(ip, port));
                }
            }
        }

        (added, dropped)
    }

    // ── BEP-9 ut_metadata ────────────────────────────────────────────

    pub fn build_metadata_request(piece: usize) -> Vec<u8> {
        use crate::bencode::{BencodeParser, BencodeValue};
        use std::collections::BTreeMap;
        let mut dict = BTreeMap::new();
        dict.insert("msg_type".to_string(), BencodeValue::Integer(0));
        dict.insert("piece".to_string(), BencodeValue::Integer(piece as i64));
        BencodeParser::encode(&BencodeValue::Dict(dict))
    }

    pub fn build_metadata_data(piece: usize, total_size: Option<usize>, data: &[u8]) -> Vec<u8> {
        use crate::bencode::{BencodeParser, BencodeValue};
        use std::collections::BTreeMap;
        let mut dict = BTreeMap::new();
        dict.insert("msg_type".to_string(), BencodeValue::Integer(1));
        dict.insert("piece".to_string(), BencodeValue::Integer(piece as i64));
        if let Some(ts) = total_size {
            dict.insert("total_size".to_string(), BencodeValue::Integer(ts as i64));
        }
        let mut payload = BencodeParser::encode(&BencodeValue::Dict(dict));
        payload.extend_from_slice(data);
        payload
    }

    pub fn build_metadata_reject(piece: usize) -> Vec<u8> {
        use crate::bencode::{BencodeParser, BencodeValue};
        use std::collections::BTreeMap;
        let mut dict = BTreeMap::new();
        dict.insert("msg_type".to_string(), BencodeValue::Integer(2));
        dict.insert("piece".to_string(), BencodeValue::Integer(piece as i64));
        BencodeParser::encode(&BencodeValue::Dict(dict))
    }

    /// Parse a ut_metadata message payload.
    /// Returns `(msg_type, piece, total_size, raw_data)`.
    /// msg_type: 0=request, 1=data, 2=reject.
    /// total_size is only meaningful for data messages (0 otherwise).
    pub fn parse_metadata_message(payload: &[u8]) -> (u8, usize, usize, Vec<u8>) {
        use crate::bencode::{BencodeParser, BencodeValue};

        let dict_end = match find_dict_end(payload) {
            Some(e) => e,
            None => return (0xFF, 0, 0, vec![]),
        };
        let raw_data = payload[dict_end..].to_vec();

        let decoded = match BencodeParser::parse(&payload[..dict_end]) {
            Ok(v) => v,
            Err(_) => return (0xFF, 0, 0, vec![]),
        };
        let dict = match decoded.as_dict() {
            Some(d) => d,
            None => return (0xFF, 0, 0, vec![]),
        };
        let msg_type = dict
            .get("msg_type")
            .and_then(|v| v.as_integer())
            .unwrap_or(0xFF) as u8;
        let piece = dict
            .get("piece")
            .and_then(|v| v.as_integer())
            .unwrap_or(0) as usize;
        let total_size = dict
            .get("total_size")
            .and_then(|v| v.as_integer())
            .unwrap_or(0) as usize;
        (msg_type, piece, total_size, raw_data)
    }
}

/// Scan to the end of the top-level bencoded dict, correctly handling
/// the length-prefixed string encoding so that `d`/`e` bytes inside
/// string values are ignored.
fn find_dict_end(data: &[u8]) -> Option<usize> {
    if data.is_empty() || data[0] != b'd' {
        return None;
    }
    let mut i = 0;
    let mut depth = 0;
    while i < data.len() {
        match data[i] {
            b'd' => depth += 1,
            b'e' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            b'i' => {
                while i < data.len() && data[i] != b'e' {
                    i += 1;
                }
            }
            b'0'..=b'9' => {
                let start = i;
                while i < data.len() && data[i] != b':' {
                    i += 1;
                }
                let len: usize =
                    std::str::from_utf8(&data[start..i]).ok()?.parse().ok()?;
                i += 1 + len; // skip ':' and the string content
                continue;
            }
            _ => return None,
        }
        i += 1;
    }
    None
}

pub struct PeerConnection {
    pub stream: TcpStream,
    pub peer_id: Option<PeerId>,
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub bitfield: Vec<u8>,
    pub addr: std::net::SocketAddrV4,
    /// Extension id the peer assigned to its `ut_pex` extension, learned
    /// from the BEP-10 extended handshake. `None` until the handshake
    /// arrives. BEP-10 lets us default to 1 if a peer sends PEX before
    /// its handshake, which is what most peers do in practice.
    pub peer_pex_id: Option<u8>,
    /// Extension id the peer assigned to its `ut_metadata` extension.
    pub peer_metadata_id: Option<u8>,
}

impl PeerConnection {
    pub async fn connect(
        addr: std::net::SocketAddrV4,
        info_hash: &InfoHash,
        my_peer_id: &PeerId,
    ) -> Result<Self> {
        let stream =
            tokio::time::timeout(std::time::Duration::from_secs(10), TcpStream::connect(addr))
                .await
                .map_err(|_| TorrentError::Timeout)?
                .map_err(|e| TorrentError::Network(e.to_string()))?;

        let _ = stream.set_nodelay(true);

        let mut conn = Self::new_connection(stream, addr);
        conn.handshake(info_hash, my_peer_id).await?;
        Ok(conn)
    }

    pub async fn accept(
        stream: TcpStream,
        addr: std::net::SocketAddrV4,
        info_hash: &InfoHash,
        my_peer_id: &PeerId,
    ) -> Result<Self> {
        let _ = stream.set_nodelay(true);

        let mut conn = Self::new_connection(stream, addr);
        conn.incoming_handshake(info_hash, my_peer_id).await?;
        Ok(conn)
    }

    fn new_connection(stream: TcpStream, addr: std::net::SocketAddrV4) -> Self {
        PeerConnection {
            stream,
            peer_id: None,
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            bitfield: Vec::new(),
            addr,
            peer_pex_id: None,
            peer_metadata_id: None,
        }
    }

    async fn handshake(&mut self, info_hash: &InfoHash, my_peer_id: &PeerId) -> Result<()> {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let mut msg = Vec::with_capacity(68);
        msg.push(19);
        msg.extend_from_slice(b"BitTorrent protocol");
        let mut reserved = [0u8; 8];
        reserved[5] = 0x10 | 0x04; // BEP-10 (extension protocol) + BEP-9 (ut_metadata)
        reserved[7] = 0x01; // BEP-5 (DHT)
        msg.extend_from_slice(&reserved);
        msg.extend_from_slice(info_hash.as_bytes());
        msg.extend_from_slice(&my_peer_id.0);

        self.stream
            .write_all(&msg)
            .await
            .map_err(|e| TorrentError::Network(e.to_string()))?;

        let mut resp = vec![0u8; 68];
        self.stream
            .read_exact(&mut resp)
            .await
            .map_err(|e| TorrentError::Network(e.to_string()))?;

        if resp[0] != 19 || &resp[1..20] != b"BitTorrent protocol" {
            return Err(TorrentError::Peer("Invalid handshake".to_string()));
        }

        if &resp[28..48] != info_hash.as_bytes() {
            return Err(TorrentError::Peer("Info hash mismatch".to_string()));
        }

        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&resp[48..68]);
        self.peer_id = Some(PeerId(peer_id));

        Ok(())
    }

    async fn incoming_handshake(
        &mut self,
        info_hash: &InfoHash,
        my_peer_id: &PeerId,
    ) -> Result<()> {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let mut resp = vec![0u8; 68];
        self.stream
            .read_exact(&mut resp)
            .await
            .map_err(|e| TorrentError::Network(e.to_string()))?;

        if resp[0] != 19 || &resp[1..20] != b"BitTorrent protocol" {
            return Err(TorrentError::Peer("Invalid handshake".to_string()));
        }

        if &resp[28..48] != info_hash.as_bytes() {
            return Err(TorrentError::Peer("Info hash mismatch".to_string()));
        }

        let mut peer_id = [0u8; 20];
        peer_id.copy_from_slice(&resp[48..68]);
        self.peer_id = Some(PeerId(peer_id));

        let mut msg = Vec::with_capacity(68);
        msg.push(19);
        msg.extend_from_slice(b"BitTorrent protocol");
        let mut reserved = [0u8; 8];
        reserved[5] = 0x10 | 0x04; // BEP-10 + BEP-9
        msg.extend_from_slice(&reserved);
        msg.extend_from_slice(info_hash.as_bytes());
        msg.extend_from_slice(&my_peer_id.0);

        self.stream
            .write_all(&msg)
            .await
            .map_err(|e| TorrentError::Network(e.to_string()))?;

        Ok(())
    }

    pub async fn send_message(&mut self, msg: &PeerMessage) -> Result<()> {
        use tokio::io::AsyncWriteExt;

        let data = msg.encode();
        self.stream
            .write_all(&data)
            .await
            .map_err(|e| TorrentError::Network(e.to_string()))?;
        Ok(())
    }

    pub async fn recv_message(&mut self) -> Result<PeerMessage> {
        use tokio::io::AsyncReadExt;

        let mut len_buf = [0u8; 4];
        self.stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| TorrentError::Network(e.to_string()))?;

        let length = u32::from_be_bytes(len_buf) as usize;

        if length == 0 {
            return Ok(PeerMessage::KeepAlive);
        }

        if length > 4 * 1024 * 1024 {
            return Err(TorrentError::Peer(format!(
                "Message too large: {} bytes",
                length
            )));
        }

        let mut payload = vec![0u8; length];
        self.stream
            .read_exact(&mut payload)
            .await
            .map_err(|e| TorrentError::Network(e.to_string()))?;

        PeerMessage::decode(&payload)
    }

    pub fn has_piece(&self, index: u32) -> bool {
        let byte_index = (index / 8) as usize;
        let bit_offset = 7 - (index % 8);

        if byte_index < self.bitfield.len() {
            (self.bitfield[byte_index] >> bit_offset) & 1 == 1
        } else {
            false
        }
    }

    pub fn set_piece(&mut self, index: u32) {
        let byte_index = (index / 8) as usize;
        let bit_offset = 7 - (index % 8);

        while self.bitfield.len() <= byte_index {
            self.bitfield.push(0);
        }
        self.bitfield[byte_index] |= 1 << bit_offset;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extended_handshake_roundtrip() {
        let payload = PeerMessage::build_extended_handshake_payload(64);
        let (pex_id, metadata_id) = PeerMessage::parse_extended_handshake(&payload);
        assert_eq!(pex_id, Some(1), "we should advertise ut_pex at id 1");
        assert_eq!(metadata_id, Some(2), "we should advertise ut_metadata at id 2");
    }

    #[test]
    fn extended_handshake_only_ut_metadata() {
        use crate::bencode::{BencodeParser, BencodeValue};
        use std::collections::BTreeMap;
        let mut m = BTreeMap::new();
        m.insert("ut_metadata".to_string(), BencodeValue::Integer(2));
        let mut top = BTreeMap::new();
        top.insert("m".to_string(), BencodeValue::Dict(m));
        let payload = BencodeParser::encode(&BencodeValue::Dict(top));
        let (pex_id, metadata_id) = PeerMessage::parse_extended_handshake(&payload);
        assert_eq!(pex_id, None);
        assert_eq!(metadata_id, Some(2));
    }
}
