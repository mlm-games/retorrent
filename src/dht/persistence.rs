use std::fs;
use std::io::{BufReader, BufWriter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace, warn};

use crate::dht::peer_store::PeerStore;
use crate::dht::routing_table::RoutingTable;
use crate::dht::{DhtNode, Error, Result};
use crate::types::InfoHash;

#[derive(Debug, Default)]
pub struct PersistentDhtConfig {
    pub dump_interval: Option<Duration>,
    pub config_filename: Option<PathBuf>,
    pub port: Option<u16>,
    pub ipv4_only: bool,
}

#[derive(Serialize, Deserialize)]
pub struct PersistedDht {
    pub node_id: String,
    pub listen_addr: String,
    pub table_v4: RoutingTable,
    #[serde(default)]
    pub table_v6: Option<RoutingTable>,
    #[serde(default)]
    pub peers: Vec<PersistedPeerEntry>,
}

#[derive(Serialize, Deserialize)]
pub struct PersistedPeerEntry {
    pub info_hash: String,
    pub addrs: Vec<SocketAddr>,
}

impl PersistedDht {
    pub fn dump(dht: &DhtNode, path: &Path) -> Result<()> {
        let tmp = tmp_path_for(path);
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp)
            .map_err(|e| Error::Persistence(e.to_string()))?;
        let mut w = BufWriter::new(file);

        let persisted = {
            let v4 = dht.routing_table_v4.read();
            let v6 = dht.routing_table_v6.read();
            let peers = dht.peer_store.collect_persisted();
            PersistedDht {
                node_id: dht.id.to_hex(),
                listen_addr: dht.listen_addr.to_string(),
                table_v4: v4.clone(),
                table_v6: Some(v6.clone()),
                peers: peers.into_iter()
                    .map(|(h, a)| PersistedPeerEntry {
                        info_hash: h.to_hex(),
                        addrs: a,
                    })
                    .collect(),
            }
        };

        if let Err(e) = serde_json::to_writer_pretty(&mut w, &persisted) {
            return Err(Error::Persistence(format!("serialize: {e}")));
        }
        drop(w);
        if let Err(e) = fs::rename(&tmp, path) {
            return Err(Error::Persistence(format!("rename: {e}")));
        }
        trace!("dumped DHT to {:?}", path);
        Ok(())
    }

    pub fn load(path: &Path) -> Option<Self> {
        let file = match fs::OpenOptions::new().read(true).open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => {
                warn!(filename=?path, "DHT: cannot open: {e}");
                return None;
            }
        };
        let reader = BufReader::new(file);
        match serde_json::from_reader::<_, PersistedDht>(reader) {
            Ok(p) => {
                info!(filename=?path, "loaded DHT routing table from disk");
                Some(p)
            }
            Err(e) => {
                warn!(filename=?path, "DHT: cannot deserialize: {e:#}");
                None
            }
        }
    }
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let pid = std::process::id();
    tmp.set_file_name(format!(
        "{}.tmp.{pid}",
        path.file_name().and_then(|s| s.to_str()).unwrap_or("dht.json")
    ));
    tmp
}

pub fn default_persistence_filename() -> Result<PathBuf> {
    let dir = crate::config::ANDROID_DATA_DIR
        .get()
        .cloned()
        .unwrap_or_else(|| {
            dirs::data_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("retorrent")
        })
        .join("dht");
    fs::create_dir_all(&dir).map_err(|e| Error::Persistence(e.to_string()))?;
    Ok(dir.join("dht.json"))
}

pub fn load_persistent_state(path: &Path) -> Option<PersistedDht> {
    PersistedDht::load(path)
}

pub fn dump_dht(dht: &DhtNode, path: &Path) {
    if let Err(e) = PersistedDht::dump(dht, path) {
        error!(filename=?path, "error dumping DHT: {e}");
    } else {
        debug!(filename=?path, "dumped DHT");
    }
}

pub fn spawn_dumper(
    dht: std::sync::Arc<DhtNode>,
    path: PathBuf,
    interval: Duration,
    cancel: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    dump_dht(&dht, &path);
                }
                _ = cancel.cancelled() => break,
            }
        }
    });
}

pub fn make_persistent_peer_store(
    node_id: InfoHash,
    persisted: &PersistedDht,
) -> PeerStore {
    let peers: Vec<(InfoHash, Vec<SocketAddr>)> = persisted.peers.iter()
        .filter_map(|e| {
            let hash = InfoHash::from_hex(&e.info_hash)?;
            Some((hash, e.addrs.clone()))
        })
        .collect();
    PeerStore::new_with_persistence(node_id, peers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::routing_table::RoutingTable;
    use crate::types::InfoHash;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn test_id(byte: u8) -> InfoHash { InfoHash([byte; 20]) }

    fn make_table() -> RoutingTable {
        let id = test_id(0xab);
        let mut t = RoutingTable::new(id);
        for i in 0..20u8 {
            let node_id = InfoHash([i.wrapping_add(1); 20]);
            let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, i), 6881));
            t.add_node(node_id, addr);
        }
        t
    }

    #[test]
    fn roundtrip_persisted_dht() {
        let node_id = test_id(0x42);
        let v4 = make_table();
        let v6 = RoutingTable::new(node_id);
        let peers = vec![
            (test_id(0x11), vec![
                SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881)),
            ]),
        ];
        let ps = PeerStore::new_with_persistence(node_id, peers);
        let listen = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), 51413));

        let persisted = PersistedDht {
            node_id: node_id.to_hex(),
            listen_addr: listen.to_string(),
            table_v4: v4,
            table_v6: Some(v6),
            peers: ps.collect_persisted().into_iter()
                .map(|(h, a)| PersistedPeerEntry { info_hash: h.to_hex(), addrs: a })
                .collect(),
        };
        let json = serde_json::to_string_pretty(&persisted).expect("serialize");
        let loaded: PersistedDht = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(loaded.node_id, node_id.to_hex());
        assert_eq!(loaded.listen_addr, listen.to_string());
        // Bucket max is 8, so 20 nodes with same prefix bucket into <= 8.
        assert!(loaded.table_v4.len() > 0 && loaded.table_v4.len() <= 8);
        let ps2 = make_persistent_peer_store(node_id, &loaded);
        let got = ps2.get_peers(test_id(0x11));
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn load_missing_file_returns_none() {
        let path = std::path::PathBuf::from("/tmp/retorrent_dht_does_not_exist.json");
        let _ = std::fs::remove_file(&path);
        assert!(PersistedDht::load(&path).is_none());
    }
}
