use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::types::InfoHash;

#[derive(Serialize, Deserialize)]
struct StoredToken {
    token: [u8; 4],
    node_id: InfoHash,
    addr: SocketAddr,
}

#[derive(Serialize, Deserialize)]
struct StoredPeer {
    addr: SocketAddr,
    time: DateTime<Utc>,
}

pub struct PeerStore {
    self_id: InfoHash,
    max_remembered_tokens: u32,
    max_remembered_peers: u32,
    max_distance: InfoHash,
    tokens: RwLock<VecDeque<StoredToken>>,
    peers: DashMap<InfoHash, Vec<StoredPeer>>,
    peers_len: AtomicU32,
}

impl Serialize for PeerStore {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("PeerStore", 5)?;
        st.serialize_field("self_id", &self.self_id.to_hex())?;
        st.serialize_field("max_remembered_tokens", &self.max_remembered_tokens)?;
        st.serialize_field("max_remembered_peers", &self.max_remembered_peers)?;
        st.serialize_field("max_distance", &self.max_distance.to_hex())?;
        st.serialize_field("peers", &self.collect_persisted())?;
        st.end()
    }
}

impl PeerStore {
    pub fn new(self_id: InfoHash) -> Self {
        Self {
            self_id,
            max_remembered_tokens: 1000,
            max_remembered_peers: 1000,
            max_distance: InfoHash::from_hex("0000ffffffffffffffffffffffffffffffffffff").unwrap(),
            tokens: RwLock::new(VecDeque::new()),
            peers: DashMap::new(),
            peers_len: AtomicU32::new(0),
        }
    }

    pub fn new_with_persistence(
        self_id: InfoHash,
        peers: Vec<(InfoHash, Vec<SocketAddr>)>,
    ) -> Self {
        let s = Self::new(self_id);
        let mut total = 0u32;
        for (info_hash, addrs) in peers {
            let stored: Vec<StoredPeer> = addrs.into_iter()
                .map(|addr| StoredPeer { addr, time: Utc::now() })
                .collect();
            total += stored.len() as u32;
            s.peers.insert(info_hash, stored);
        }
        s.peers_len.store(total, Ordering::SeqCst);
        s
    }

    pub fn collect_persisted(&self) -> Vec<(InfoHash, Vec<SocketAddr>)> {
        self.peers.iter()
            .map(|entry| (entry.key().clone(), entry.value().iter().map(|p| p.addr).collect()))
            .collect()
    }

    /// Generate a fresh token for a node that just queried us for peers.
    ///
    /// **Tokens are intentionally ephemeral.** Each token is 4 random bytes
    /// stored only in this in-memory list, valid for as long as we keep it
    /// around (and dropped when the list overflows at `max_remembered_tokens`).
    /// They are not persisted, not derived from any long-lived key, and a
    /// restart invalidates all outstanding tokens. This is correct: announce_peer
    /// must be sent within seconds of get_peers, and the spec does not require
    /// any cross-restart continuity. Rotating tokens on restart prevents stale
    /// tokens from being usable if a node's state is exfiltrated.
    pub fn gen_token_for(&self, node_id: InfoHash, addr: SocketAddr) -> [u8; 4] {
        let mut token = [0u8; 4];
        rand::rng().fill_bytes(&mut token);
        let mut tokens = self.tokens.write();
        tokens.push_back(StoredToken { token, addr, node_id });
        if tokens.len() > self.max_remembered_tokens as usize {
            tokens.pop_front();
        }
        token
    }

    pub fn store_peer(&self, id: InfoHash, info_hash: InfoHash, token: &[u8], port: u16, implied_port: u8, mut addr: SocketAddr) -> bool {
        if info_hash.distance(&self.self_id) > self.max_distance {
            trace!("peer store: info_hash too far");
            return false;
        }
        if !self.tokens.read().iter().any(|t| {
            t.token.as_slice() == token && t.addr == addr && t.node_id == id
        }) {
            trace!("peer store: token mismatch");
            return false;
        }
        if implied_port == 0 {
            addr.set_port(port);
        }
        use dashmap::mapref::entry::Entry;
        match self.peers.entry(info_hash) {
            Entry::Occupied(mut occ) => {
                if let Some(s) = occ.get_mut().iter_mut().find(|s| s.addr == addr) {
                    s.time = Utc::now();
                    return true;
                }
                if self.peers_len.load(Ordering::SeqCst) >= self.max_remembered_peers {
                    return false;
                }
                occ.get_mut().push(StoredPeer { addr, time: Utc::now() });
            }
            Entry::Vacant(vac) => {
                if self.peers_len.load(Ordering::SeqCst) >= self.max_remembered_peers {
                    return false;
                }
                vac.insert(vec![StoredPeer { addr, time: Utc::now() }]);
            }
        }
        self.peers_len.fetch_add(1, Ordering::SeqCst);
        true
    }

    pub fn get_peers(&self, info_hash: InfoHash) -> Vec<SocketAddr> {
        if let Some(peers) = self.peers.get(&info_hash) {
            return peers.iter().map(|p| p.addr).collect();
        }
        Vec::new()
    }
}
