use crate::config::Config;
use crate::error::Result;
use crate::metainfo::MetaInfo;
use crate::network::TorrentSession;
use crate::types::*;
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct TorrentEngine {
    pub config: Arc<Config>,
    pub peer_id: PeerId,
    pub sessions: Arc<DashMap<InfoHash, Arc<TorrentSession>>>,
}

impl TorrentEngine {
    pub async fn new(config: Config) -> Self {
        std::fs::create_dir_all(&config.download_dir).ok();
        std::fs::create_dir_all(Config::resume_dir()).ok();

        let engine = Self {
            config: Arc::new(config),
            peer_id: PeerId::generate(),
            sessions: Arc::new(DashMap::new()),
        };

        if engine.config.auto_resume {
            engine.load_resume_data();
        }

        engine
    }

    pub fn add_torrent_from_bytes(&self, data: Vec<u8>) -> Result<InfoHash> {
        let meta = MetaInfo::from_bytes(&data)?;
        let info_hash = meta.info_hash;

        if self.sessions.contains_key(&info_hash) {
            return Ok(info_hash);
        }

        let session = TorrentSession::new(
            meta,
            self.config.download_dir.clone(),
            self.peer_id,
            self.config.clone(),
        )?;

        if let Some(rd) = ResumeData::load_from_dir(&info_hash.to_hex(), &Config::resume_dir()) {
            session.apply_resume(&rd);
            tracing::info!("Restored resume data for {}", info_hash);
        }

        session.set_torrent_bytes(data);
        let rd = session.snapshot_resume();
        let _ = rd.save_to_dir(&Config::resume_dir());

        self.sessions.insert(info_hash, session);
        Ok(info_hash)
    }

    pub fn add_torrent_from_magnet(&self, uri: &str) -> Result<InfoHash> {
        let magnet = MagnetLink::parse(uri)?;
        let info_hash = magnet.info_hash;

        if self.sessions.contains_key(&info_hash) {
            return Ok(info_hash);
        }

        let name = magnet.display_name.unwrap_or_else(|| info_hash.to_hex());

        let mut announce_list: Vec<Vec<String>> = Vec::new();
        if !magnet.trackers.is_empty() {
            announce_list.push(magnet.trackers.clone());
        }

        let meta = MetaInfo {
            info_hash,
            name: name.clone(),
            piece_length: 0,
            pieces: Vec::new(),
            files: Vec::new(),
            total_size: 0,
            announce: magnet.trackers.first().cloned(),
            announce_list,
            comment: None,
            created_by: None,
            creation_date: None,
            is_private: false,
        };

        let session = TorrentSession::new(
            meta,
            self.config.download_dir.clone(),
            self.peer_id,
            self.config.clone(),
        )?;

        session.stats.lock().state = TorrentState::FetchingMetadata;

        self.sessions.insert(info_hash, session);
        tracing::info!("Added magnet torrent: {} ({})", name, info_hash);
        Ok(info_hash)
    }

    pub fn start_torrent(&self, info_hash: &InfoHash, rt: &tokio::runtime::Runtime) {
        if let Some(session) = self.sessions.get(info_hash) {
            let session = session.value().clone();
            let max_peers = self.config.max_connections_per_torrent;
            rt.spawn(async move {
                session.start(max_peers).await;
            });
        }
    }

    pub fn pause_torrent(&self, info_hash: &InfoHash) {
        if let Some(session) = self.sessions.get(info_hash) {
            session.pause();
        }
    }

    pub fn resume_torrent(&self, info_hash: &InfoHash) {
        if let Some(session) = self.sessions.get(info_hash) {
            session.resume();
        }
    }

    pub fn remove_torrent(&self, info_hash: &InfoHash, delete_files: bool) {
        if let Some((_, session)) = self.sessions.remove(info_hash) {
            session.stop();
            if delete_files {
                let _ = session.storage.delete_files();
            }
            let _ = ResumeData::remove_from_dir(&info_hash.to_hex(), &Config::resume_dir());
        }
    }

    pub fn set_file_priority(
        &self,
        info_hash: &InfoHash,
        file_index: usize,
        priority: FilePriority,
    ) {
        if let Some(session) = self.sessions.get(info_hash) {
            session.set_file_priority(file_index, priority);
        }
    }

    fn load_resume_data(&self) {
        let dir = Config::resume_dir();
        let all = ResumeData::list_all(&dir);
        for rd in all {
            if let Some(torrent_bytes) = &rd.torrent_bytes {
                match MetaInfo::from_bytes(torrent_bytes) {
                    Ok(meta) => {
                        let info_hash = meta.info_hash;
                        match TorrentSession::new(
                            meta,
                            self.config.download_dir.clone(),
                            self.peer_id,
                            self.config.clone(),
                        ) {
                            Ok(session) => {
                                session.apply_resume(&rd);
                                session.set_torrent_bytes(torrent_bytes.clone());
                                self.sessions.insert(info_hash, session);
                                tracing::info!("Resumed torrent: {}", info_hash);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    "Failed to create session for {}: {}",
                                    rd.info_hash,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse stored torrent {}: {}", rd.info_hash, e);
                    }
                }
            }
        }
    }

    pub fn apply_config(&self, new_config: &Config) {
        for entry in self.sessions.iter() {
            let session = entry.value();
            session.dl_limiter.set_rate(new_config.max_download_rate);
            session.ul_limiter.set_rate(new_config.max_upload_rate);
        }
    }

    pub fn save_all_resume(&self) {
        let dir = Config::resume_dir();
        for entry in self.sessions.iter() {
            let session = entry.value();
            let rd = session.snapshot_resume();
            if let Err(e) = rd.save_to_dir(&dir) {
                tracing::warn!("Failed to save resume for {}: {}", entry.key(), e);
            }
        }
    }

    pub fn get_all_info_hashes(&self) -> Vec<InfoHash> {
        self.sessions.iter().map(|entry| *entry.key()).collect()
    }

    pub fn get_session(&self, info_hash: &InfoHash) -> Option<Arc<TorrentSession>> {
        self.sessions.get(info_hash).map(|s| s.value().clone())
    }
}
