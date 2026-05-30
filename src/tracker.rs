use crate::bencode::BencodeParser;
use crate::error::{Result, TorrentError};
use crate::types::{InfoHash, PeerId};
use std::net::{Ipv4Addr, SocketAddrV4};

#[derive(Debug, Clone)]
pub struct TrackerResponse {
    pub interval: u64,
    pub min_interval: Option<u64>,
    pub peers: Vec<SocketAddrV4>,
    pub seeders: Option<u64>,
    pub leechers: Option<u64>,
    #[allow(dead_code)]
    pub warning: Option<String>,
}

pub struct TrackerClient {
    http: reqwest::Client,
}

impl TrackerClient {
    pub fn new() -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }

    pub async fn announce(
        &self,
        announce_url: &str,
        info_hash: &InfoHash,
        peer_id: &PeerId,
        port: u16,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: Option<&str>,
    ) -> Result<TrackerResponse> {
        if announce_url.starts_with("http://") || announce_url.starts_with("https://") {
            self.http_announce(
                announce_url,
                info_hash,
                peer_id,
                port,
                uploaded,
                downloaded,
                left,
                event,
            )
            .await
        } else if announce_url.starts_with("udp://") {
            self.udp_announce(
                announce_url,
                info_hash,
                peer_id,
                port,
                uploaded,
                downloaded,
                left,
                event,
            )
            .await
        } else {
            Err(TorrentError::Tracker(format!(
                "Unsupported tracker protocol: {}",
                announce_url
            )))
        }
    }

    async fn http_announce(
        &self,
        announce_url: &str,
        info_hash: &InfoHash,
        peer_id: &PeerId,
        port: u16,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: Option<&str>,
    ) -> Result<TrackerResponse> {
        let info_hash_encoded = urlencoding_bytes(info_hash.as_bytes());
        let peer_id_encoded = urlencoding_bytes(&peer_id.0);

        let sep = if announce_url.contains('?') {
            "&"
        } else {
            "?"
        };

        let mut url = format!(
            "{}{}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant=100",
            announce_url, sep, info_hash_encoded, peer_id_encoded, port, uploaded, downloaded, left
        );

        if let Some(ev) = event {
            url.push_str("&event=");
            url.push_str(ev);
        }

        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let body = resp
            .bytes()
            .await
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let decoded = BencodeParser::parse(&body)?;

        if let Some(failure) = decoded.dict_get("failure reason") {
            if let Some(msg) = failure.as_string() {
                return Err(TorrentError::Tracker(msg.to_string()));
            }
        }

        let interval = decoded
            .dict_get("interval")
            .and_then(|v| v.as_integer())
            .unwrap_or(1800) as u64;

        let min_interval = decoded
            .dict_get("min interval")
            .and_then(|v| v.as_integer())
            .map(|v| v as u64);

        let seeders = decoded
            .dict_get("complete")
            .and_then(|v| v.as_integer())
            .map(|v| v as u64);

        let leechers = decoded
            .dict_get("incomplete")
            .and_then(|v| v.as_integer())
            .map(|v| v as u64);

        let warning = decoded
            .dict_get("warning message")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let peers = self.parse_peers(&decoded)?;

        Ok(TrackerResponse {
            interval,
            min_interval,
            peers,
            seeders,
            leechers,
            warning,
        })
    }

    fn parse_peers(
        &self,
        response: &crate::bencode::BencodeValue,
    ) -> Result<Vec<SocketAddrV4>> {
        let peers_val = response
            .dict_get("peers")
            .ok_or_else(|| TorrentError::Tracker("Missing peers".to_string()))?;

        match peers_val {
            crate::bencode::BencodeValue::ByteString(data) => {
                let mut peers = Vec::new();
                for chunk in data.chunks_exact(6) {
                    let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
                    let port = u16::from_be_bytes([chunk[4], chunk[5]]);
                    peers.push(SocketAddrV4::new(ip, port));
                }
                Ok(peers)
            }
            crate::bencode::BencodeValue::List(list) => {
                let mut peers = Vec::new();
                for item in list {
                    if let Some(dict) = item.as_dict() {
                        let ip_str = dict
                            .get("ip")
                            .and_then(|v| v.as_string())
                            .unwrap_or("0.0.0.0");
                        let port = dict
                            .get("port")
                            .and_then(|v| v.as_integer())
                            .unwrap_or(0) as u16;

                        if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                            peers.push(SocketAddrV4::new(ip, port));
                        }
                    }
                }
                Ok(peers)
            }
            _ => Err(TorrentError::Tracker("Invalid peers format".to_string())),
        }
    }

    async fn udp_announce(
        &self,
        announce_url: &str,
        info_hash: &InfoHash,
        peer_id: &PeerId,
        port: u16,
        uploaded: u64,
        downloaded: u64,
        left: u64,
        event: Option<&str>,
    ) -> Result<TrackerResponse> {
        use tokio::net::UdpSocket;
        use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
        use std::io::Cursor;

        let url = url::Url::parse(announce_url)
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let host = url.host_str().unwrap_or("127.0.0.1");
        let port_tracker = url.port().unwrap_or(80);
        let addr = format!("{}:{}", host, port_tracker);

        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;
        socket
            .connect(&addr)
            .await
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let transaction_id: u32 = rand::random();
        let mut connect_req = Vec::with_capacity(16);
        connect_req
            .write_u64::<BigEndian>(0x41727101980)
            .unwrap();
        connect_req.write_u32::<BigEndian>(0).unwrap();
        connect_req
            .write_u32::<BigEndian>(transaction_id)
            .unwrap();

        socket
            .send(&connect_req)
            .await
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let mut buf = vec![0u8; 8192];
        let timeout = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            socket.recv(&mut buf),
        )
        .await
        .map_err(|_| TorrentError::Timeout)?
        .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let mut cursor = Cursor::new(&buf[..timeout]);
        let action = cursor
            .read_u32::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;
        let txn = cursor
            .read_u32::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        if action != 0 {
            return Err(TorrentError::Tracker("Connect action failed".to_string()));
        }
        if txn != transaction_id {
            return Err(TorrentError::Tracker("Transaction ID mismatch on connect".to_string()));
        }

        let connection_id = cursor
            .read_u64::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let transaction_id2: u32 = rand::random();
        let mut announce_req = Vec::with_capacity(98);
        announce_req
            .write_u64::<BigEndian>(connection_id)
            .unwrap();
        announce_req.write_u32::<BigEndian>(1).unwrap();
        announce_req
            .write_u32::<BigEndian>(transaction_id2)
            .unwrap();
        announce_req.extend_from_slice(info_hash.as_bytes());
        announce_req.extend_from_slice(&peer_id.0);
        announce_req.write_u64::<BigEndian>(downloaded).unwrap();
        announce_req.write_u64::<BigEndian>(left).unwrap();
        announce_req.write_u64::<BigEndian>(uploaded).unwrap();

        let event_num: u32 = match event {
            Some("started") => 2,
            Some("stopped") => 3,
            Some("completed") => 1,
            _ => 0,
        };
        announce_req.write_u32::<BigEndian>(event_num).unwrap();
        announce_req.write_u32::<BigEndian>(0).unwrap();
        announce_req.write_u32::<BigEndian>(rand::random()).unwrap();
        announce_req.write_i32::<BigEndian>(100).unwrap();
        announce_req.write_u16::<BigEndian>(port).unwrap();

        socket
            .send(&announce_req)
            .await
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let n = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            socket.recv(&mut buf),
        )
        .await
        .map_err(|_| TorrentError::Timeout)?
        .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        let mut cursor = Cursor::new(&buf[..n]);
        let action = cursor
            .read_u32::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;
        let txn2 = cursor
            .read_u32::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))?;

        if action != 1 {
            return Err(TorrentError::Tracker(
                "Announce action failed".to_string(),
            ));
        }
        if txn2 != transaction_id2 {
            return Err(TorrentError::Tracker("Transaction ID mismatch on announce".to_string()));
        }

        let interval = cursor
            .read_u32::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))? as u64;
        let leechers = cursor
            .read_u32::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))? as u64;
        let seeders = cursor
            .read_u32::<BigEndian>()
            .map_err(|e| TorrentError::Tracker(e.to_string()))? as u64;

        let remaining = &buf[20..n];
        if remaining.len() % 6 != 0 {
            return Err(TorrentError::Tracker(format!(
                "UDP tracker response peer data length {} not a multiple of 6",
                remaining.len()
            )));
        }
        let mut peers = Vec::new();
        for chunk in remaining.chunks_exact(6) {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            peers.push(SocketAddrV4::new(ip, port));
        }

        Ok(TrackerResponse {
            interval,
            min_interval: None,
            peers,
            seeders: Some(seeders),
            leechers: Some(leechers),
            warning: None,
        })
    }
}

fn urlencoding_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|&b| {
            if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.' || b == b'~' {
                format!("{}", b as char)
            } else {
                format!("%{:02X}", b)
            }
        })
        .collect()
}
