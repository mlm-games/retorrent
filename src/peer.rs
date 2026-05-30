use crate::error::{Result, TorrentError};
use crate::types::*;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddrV4};
use tokio::net::TcpStream;

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
        }
    }

    async fn handshake(&mut self, info_hash: &InfoHash, my_peer_id: &PeerId) -> Result<()> {
        use tokio::io::AsyncReadExt;
        use tokio::io::AsyncWriteExt;

        let mut msg = Vec::with_capacity(68);
        msg.push(19);
        msg.extend_from_slice(b"BitTorrent protocol");
        let mut reserved = [0u8; 8];
        reserved[5] = 0x10; // advertise extension protocol (BEP-10)
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
        reserved[5] = 0x10;
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
