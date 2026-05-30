use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InfoHash(pub [u8; 20]);

impl InfoHash {
    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    pub fn from_hex(s: &str) -> Option<Self> {
        let bytes = hex::decode(s).ok()?;
        if bytes.len() != 20 {
            return None;
        }
        let mut hash = [0u8; 20];
        hash.copy_from_slice(&bytes);
        Some(InfoHash(hash))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl fmt::Debug for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InfoHash({})", hex::encode(self.0))
    }
}

impl fmt::Display for InfoHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId(pub [u8; 20]);

impl PeerId {
    pub fn generate() -> Self {
        let mut id = [0u8; 20];
        let prefix = b"-RE0100-";
        id[..8].copy_from_slice(prefix);
        for byte in &mut id[8..] {
            *byte = rand::random();
        }
        PeerId(id)
    }
}

impl fmt::Debug for PeerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PeerId({})", String::from_utf8_lossy(&self.0[..8]))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TorrentState {
    Checking,
    Downloading,
    Seeding,
    Paused,
    Queued,
    Error,
    Complete,
    FetchingMetadata,
}

impl fmt::Display for TorrentState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Checking => write!(f, "Checking"),
            Self::Downloading => write!(f, "Downloading"),
            Self::Seeding => write!(f, "Seeding"),
            Self::Paused => write!(f, "Paused"),
            Self::Queued => write!(f, "Queued"),
            Self::Error => write!(f, "Error"),
            Self::Complete => write!(f, "Complete"),
            Self::FetchingMetadata => write!(f, "Fetching Metadata"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilePriority {
    Skip,
    Low,
    Normal,
    High,
}

impl Default for FilePriority {
    fn default() -> Self {
        FilePriority::Normal
    }
}

impl fmt::Display for FilePriority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Skip => write!(f, "Skip"),
            Self::Low => write!(f, "Low"),
            Self::Normal => write!(f, "Normal"),
            Self::High => write!(f, "High"),
        }
    }
}

pub const BLOCK_SIZE: u32 = 16384;

#[derive(Debug, Clone)]
pub struct MagnetLink {
    pub info_hash: InfoHash,
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
    #[allow(dead_code)]
    pub web_seeds: Vec<String>,
}

impl MagnetLink {
    pub fn parse(uri: &str) -> crate::error::Result<Self> {
        if !uri.starts_with("magnet:?") {
            return Err(crate::error::TorrentError::MagnetParse(
                "Not a magnet URI".into(),
            ));
        }

        let query = &uri["magnet:?".len()..];
        let mut info_hash: Option<InfoHash> = None;
        let mut display_name: Option<String> = None;
        let mut trackers: Vec<String> = Vec::new();
        let mut web_seeds: Vec<String> = Vec::new();

        for pair in query.split('&') {
            let mut kv = pair.splitn(2, '=');
            let key = kv.next().unwrap_or("");
            let val = kv.next().unwrap_or("");
            let decoded = urlencoding::decode(val).unwrap_or_default().to_string();

            match key {
                "xt" => {
                    if let Some(hex_str) = decoded.strip_prefix("urn:btih:") {
                        if hex_str.len() == 40 {
                            if let Some(h) = InfoHash::from_hex(hex_str) {
                                info_hash = Some(h);
                            }
                        } else if hex_str.len() == 32 {
                            if let Some(bytes) = base32_decode(hex_str) {
                                if bytes.len() == 20 {
                                    let mut hash = [0u8; 20];
                                    hash.copy_from_slice(&bytes);
                                    info_hash = Some(InfoHash(hash));
                                }
                            }
                        }
                    }
                }
                "dn" => display_name = Some(decoded),
                "tr" => trackers.push(decoded),
                "ws" => web_seeds.push(decoded),
                _ => {}
            }
        }

        let info_hash = info_hash.ok_or_else(|| {
            crate::error::TorrentError::MagnetParse("No valid info hash found".into())
        })?;

        Ok(MagnetLink {
            info_hash,
            display_name,
            trackers,
            web_seeds,
        })
    }
}

fn base32_decode(input: &str) -> Option<Vec<u8>> {
    let input = input.to_uppercase();
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut bits: Vec<u8> = Vec::new();
    for c in input.bytes() {
        let val = alphabet.iter().position(|&b| b == c)?;
        for i in (0..5).rev() {
            bits.push((val >> i) as u8 & 1);
        }
    }
    let mut result = Vec::new();
    for chunk in bits.chunks(8) {
        if chunk.len() == 8 {
            let byte = chunk.iter().enumerate().fold(0u8, |acc, (i, &b)| {
                acc | (b << (7 - i))
            });
            result.push(byte);
        }
    }
    Some(result)
}

pub struct RateLimiter {
    max_rate: AtomicU64,
    tokens: AtomicU64,
    last_refill: parking_lot::Mutex<Instant>,
    fraction: parking_lot::Mutex<f64>,
}

impl RateLimiter {
    pub fn new(max_rate: u64) -> Self {
        let burst = if max_rate == 0 {
            u64::MAX
        } else {
            max_rate.max(BLOCK_SIZE as u64 * 4)
        };
        Self {
            max_rate: AtomicU64::new(max_rate),
            tokens: AtomicU64::new(burst),
            last_refill: parking_lot::Mutex::new(Instant::now()),
            fraction: parking_lot::Mutex::new(0.0),
        }
    }

    pub fn set_rate(&self, rate: u64) {
        self.max_rate.store(rate, Ordering::Relaxed);
    }

    pub fn try_consume(&self, amount: u64) -> bool {
        let max_rate = self.max_rate.load(Ordering::Relaxed);
        if max_rate == 0 {
            return true;
        }
        self.refill();
        self.tokens
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if cur >= amount {
                    Some(cur - amount)
                } else {
                    None
                }
            })
            .is_ok()
    }

    pub async fn wait_consume(&self, amount: u64) {
        let max_rate = self.max_rate.load(Ordering::Relaxed);
        if max_rate == 0 {
            return;
        }
        loop {
            if self.try_consume(amount) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    fn refill(&self) {
        let max_rate = self.max_rate.load(Ordering::Relaxed);
        let mut last = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last);
        if elapsed.is_zero() {
            return;
        }
        *last = now;

        let exact_add = max_rate as f64 * elapsed.as_secs_f64();
        let mut frac = self.fraction.lock();
        *frac += exact_add;
        let whole = *frac as u64;
        *frac -= whole as f64;

        if whole > 0 {
            let cap = max_rate.max(BLOCK_SIZE as u64 * 4);
            self.tokens
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_add(whole).min(cap))
                })
                .ok();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeData {
    pub info_hash: String,
    pub have_pieces: Vec<bool>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub file_priorities: Vec<FilePriority>,
    pub added_time: i64,
    pub completed_time: Option<i64>,
    pub torrent_bytes: Option<Vec<u8>>,
}

impl ResumeData {
    pub fn save_to_dir(&self, dir: &std::path::Path) -> crate::error::Result<()> {
        std::fs::create_dir_all(dir).map_err(|e| {
            crate::error::TorrentError::ResumeData(format!("create dir: {}", e))
        })?;
        let path = dir.join(format!("{}.resume.json", self.info_hash));
        let json = serde_json::to_string_pretty(self).map_err(|e| {
            crate::error::TorrentError::ResumeData(format!("serialize: {}", e))
        })?;
        std::fs::write(&path, json).map_err(|e| {
            crate::error::TorrentError::ResumeData(format!("write: {}", e))
        })?;
        Ok(())
    }

    pub fn load_from_dir(
        info_hash_hex: &str,
        dir: &std::path::Path,
    ) -> Option<Self> {
        let path = dir.join(format!("{}.resume.json", info_hash_hex));
        let json = std::fs::read_to_string(&path).ok()?;
        serde_json::from_str(&json).ok()
    }

    pub fn remove_from_dir(
        info_hash_hex: &str,
        dir: &std::path::Path,
    ) -> crate::error::Result<()> {
        let path = dir.join(format!("{}.resume.json", info_hash_hex));
        if path.exists() {
            std::fs::remove_file(path).map_err(|e| {
                crate::error::TorrentError::ResumeData(format!("remove: {}", e))
            })?;
        }
        Ok(())
    }

    pub fn list_all(dir: &std::path::Path) -> Vec<Self> {
        let mut results = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path
                    .extension()
                    .map(|e| e == "json")
                    .unwrap_or(false)
                {
                    if let Ok(json) = std::fs::read_to_string(&path) {
                        if let Ok(data) = serde_json::from_str::<ResumeData>(&json) {
                            results.push(data);
                        }
                    }
                }
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magnet_hex() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709&dn=Test";
        let m = MagnetLink::parse(uri).unwrap();
        assert_eq!(m.display_name.as_deref(), Some("Test"));
    }

    #[test]
    fn test_magnet_with_trackers() {
        let uri = "magnet:?xt=urn:btih:da39a3ee5e6b4b0d3255bfef95601890afd80709\
                   &tr=http%3A%2F%2Ftracker.example.com%2Fannounce\
                   &tr=udp%3A%2F%2Ftracker2.example.com%3A6969";
        let m = MagnetLink::parse(uri).unwrap();
        assert_eq!(m.trackers.len(), 2);
    }

    #[test]
    fn test_infohash_hex_roundtrip() {
        let hash = InfoHash([0xAB; 20]);
        let hex_str = hash.to_hex();
        let back = InfoHash::from_hex(&hex_str).unwrap();
        assert_eq!(hash, back);
    }
}
