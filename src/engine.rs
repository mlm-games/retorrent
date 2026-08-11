use crate::config::Config;
use crate::dht::DhtNode;
use crate::error::Result;
use crate::metainfo::MetaInfo;
use crate::network::TorrentSession;
use crate::peer::PeerConnection;
use crate::types::*;
use dashmap::DashMap;
use std::fs::create_dir_all;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct TorrentEngine {
    pub config: Arc<RwLock<Config>>,
    pub peer_id: PeerId,
    pub sessions: Arc<DashMap<InfoHash, Arc<TorrentSession>>>,
    pub dht: Option<Arc<DhtNode>>,
    pub cancellation_token: CancellationToken,
    rt: tokio::runtime::Handle,
}

impl TorrentEngine {
    pub fn config_read(&self) -> RwLockReadGuard<'_, Config> {
        self.config.read().expect("engine config poisoned")
    }

    fn config_write(&self) -> RwLockWriteGuard<'_, Config> {
        self.config.write().expect("engine config poisoned")
    }

    pub async fn new(config: Config) -> (Self, Vec<InfoHash>) {
        create_dir_all(&config.download_dir).ok();
        create_dir_all(Config::resume_dir()).ok();

        let dht = if config.dht_enabled {
            match crate::dht::DhtBuilder::with_port(config.listen_port).await {
                Ok(node) => {
                    tracing::info!("DHT started on {}", node.listen_addr());
                    Some(node)
                }
                Err(e) => {
                    tracing::warn!("DHT failed to start on :{}: {}", config.listen_port, e);
                    None
                }
            }
        } else {
            tracing::info!("DHT disabled");
            None
        };

        let engine = Self {
            config: Arc::new(RwLock::new(config)),
            peer_id: PeerId::generate(),
            sessions: Arc::new(DashMap::new()),
            dht,
            cancellation_token: CancellationToken::new(),
            rt: tokio::runtime::Handle::current(),
        };

        // Global download queue: periodically (re)enforce `max_active_downloads`.
        // Running inside `TorrentEngine::new` (which is awaited within the app
        // runtime) lets us `tokio::spawn` a self-rescheduling loop.
        if engine.config_read().max_active_downloads > 0 {
            let qe = engine.clone();
            let cancel = engine.cancellation_token.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(5));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        _ = tick.tick() => {
                            qe.enforce_queue_with_handle(&qe.rt);
                        }
                    }
                }
            });
        }

        if engine.config_read().upnp_enabled {
            let cancel = engine.cancellation_token.clone();
            let port = engine.config_read().listen_port;
            tokio::spawn(async move { crate::nat::run(port, cancel).await });
        }

        if engine.config_read().accept_incoming {
            let port = engine.config_read().listen_port;
            let cancel = engine.cancellation_token.clone();
            let sessions = engine.sessions.clone();
            let peer_id = engine.peer_id;
            tokio::spawn(async move {
                let bind = format!("0.0.0.0:{}", port);
                let listener = match TcpListener::bind(&bind).await {
                    Ok(l) => l,
                    Err(e) => {
                        tracing::warn!("Failed to bind listener on {}: {}", bind, e);
                        return;
                    }
                };
                tracing::info!("Listening for incoming peers on {}", bind);
                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => break,
                        acc = listener.accept() => {
                            match acc {
                                Ok((stream, addr)) => {
                                    let SocketAddr::V4(v4) = addr else { continue };
                                    let sessions = sessions.clone();
                                    let peer_id = peer_id;
                                    tokio::spawn(async move {
                                        match PeerConnection::accept_any(stream, v4, &peer_id).await
                                        {
                                            Ok((conn, remote_ih)) => {
                                                if let Some(session) = sessions.get(&remote_ih) {
                                                    let session = session.value().clone();
                                                    session.handle_incoming(conn).await;
                                                } else {
                                                    tracing::debug!(
                                                        "incoming peer {} for unknown torrent {}",
                                                        v4,
                                                        remote_ih
                                                    );
                                                }
                                            }
                                            Err(e) => tracing::debug!(
                                                "incoming handshake {}: {}",
                                                v4,
                                                e
                                            ),
                                        }
                                    });
                                }
                                Err(e) => tracing::debug!("accept: {}", e),
                            }
                        }
                    }
                }
            });
        }

        let to_start = if engine.config_read().auto_resume {
            engine.load_resume_data()
        } else {
            Vec::new()
        };

        (engine, to_start)
    }

    pub fn add_torrent_from_bytes(
        &self,
        data: Vec<u8>,
        download_dir: Option<PathBuf>,
        file_priorities: Option<&[FilePriority]>,
    ) -> Result<InfoHash> {
        let meta = MetaInfo::from_bytes(&data)?;
        let info_hash = meta.info_hash;

        if self.sessions.contains_key(&info_hash) {
            return Ok(info_hash);
        }

        let dir = download_dir.unwrap_or_else(|| self.config_read().download_dir.clone());
        if let Err(e) = create_dir_all(&dir) {
            tracing::warn!("Failed to create download dir {:?}: {}", dir, e);
        }

        let session = TorrentSession::new(
            meta,
            dir,
            self.peer_id,
            Arc::new(self.config_read().clone()),
            file_priorities,
        )?;

        if let Some(rd) = ResumeData::load_from_dir(&info_hash.to_hex(), &Config::resume_dir()) {
            session.apply_resume(&rd);
            tracing::info!("Restored resume data for {}", info_hash);
        }

        // Re-apply user's file priorities after resume data so they take precedence.
        if let Some(priorities) = file_priorities {
            for (i, &p) in priorities.iter().enumerate() {
                session.set_file_priority(i, p);
            }
        }

        session.set_torrent_bytes(data);
        let rd = session.snapshot_resume();
        let _ = rd.save_to_dir(&Config::resume_dir());

        self.sessions.insert(info_hash, session);
        Ok(info_hash)
    }

    pub fn add_torrent_from_magnet(
        &self,
        uri: &str,
        download_dir: Option<PathBuf>,
    ) -> Result<InfoHash> {
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
            url_list: magnet.web_seeds.clone(),
            comment: None,
            created_by: None,
            creation_date: None,
            is_private: false,
        };

        let dir = download_dir.unwrap_or_else(|| self.config_read().download_dir.clone());
        if let Err(e) = create_dir_all(&dir) {
            tracing::warn!("Failed to create download dir {:?}: {}", dir, e);
        }

        let session = TorrentSession::new(
            meta,
            dir,
            self.peer_id,
            Arc::new(self.config_read().clone()),
            None,
        )?;

        session.stats.lock().state = TorrentState::FetchingMetadata;

        self.sessions.insert(info_hash, session);
        tracing::info!("Added magnet torrent: {} ({})", name, info_hash);
        Ok(info_hash)
    }

    pub fn start_torrent(&self, info_hash: &InfoHash, rt: &tokio::runtime::Runtime) {
        self.spawn_start(info_hash, rt.handle());
    }

    /// Spawn the session's worker loops onto `handle`. Safe to call multiple
    /// times: `TorrentSession::start` guards against double-starts.
    fn spawn_start(&self, info_hash: &InfoHash, handle: &tokio::runtime::Handle) {
        if let Some(session) = self.sessions.get(info_hash) {
            let session = session.value().clone();
            let max_peers = self.config_read().max_connections_per_torrent;
            let dht = self.dht.clone();
            handle.spawn(async move {
                session.start(max_peers, dht).await;
            });
        }
    }

    pub fn pause_torrent(&self, info_hash: &InfoHash) {
        if let Some(session) = self.sessions.get(info_hash) {
            // A user pause overrides any queue-imposed pause.
            session.set_queue_paused(false);
            session.pause();
        }
    }

    pub fn resume_torrent(&self, info_hash: &InfoHash, rt: &tokio::runtime::Runtime) {
        if let Some(session) = self.sessions.get(info_hash) {
            session.resume();
        }
        self.start_torrent(info_hash, rt);
    }

    pub fn set_sequential(&self, info_hash: &InfoHash, enabled: bool) {
        if let Some(session) = self.sessions.get(info_hash) {
            session.set_sequential(enabled);
        }
    }

    /// Pause the torrent and remove it from the queue so it won't auto-start.
    pub fn queue_pause_torrent(&self, info_hash: &InfoHash) {
        if let Some(session) = self.sessions.get(info_hash) {
            session.set_queue_paused(true);
            session.pause();
            session.stats.lock().state = TorrentState::Queued;
        }
    }

    /// Enforce `max_active_downloads`: keep the most-progressed torrents
    /// downloading, pause the rest as `Queued`, and auto-resume/start queued
    /// torrents when a slot frees (e.g. another torrent completes).
    pub fn enforce_queue(&self, rt: &tokio::runtime::Runtime) {
        self.enforce_queue_with_handle(rt.handle());
    }

    fn enforce_queue_with_handle(&self, handle: &tokio::runtime::Handle) {
        let max_dl = self.config_read().max_active_downloads;
        if max_dl == 0 {
            // 0 = unlimited: lift any queue-imposed pauses.
            for entry in self.sessions.iter() {
                let s = entry.value();
                if s.is_queue_paused() {
                    s.resume();
                    if !s.is_started() {
                        self.spawn_start(entry.key(), handle);
                    }
                }
            }
            return;
        }

        struct Candidate {
            hash: InfoHash,
            session: Arc<TorrentSession>,
            progress: f32,
        }

        let mut candidates: Vec<Candidate> = Vec::new();
        let mut completed_but_queued: Vec<InfoHash> = Vec::new();

        for entry in self.sessions.iter() {
            let s = entry.value();
            let complete = s.piece_manager.read().is_complete();
            if s.is_queue_paused() && complete {
                completed_but_queued.push(*entry.key());
            }
            // Completed torrents seed regardless; user-paused torrents are
            // never counted against download slots nor auto-resumed.
            let user_paused = s.paused.load(Ordering::Relaxed) && !s.is_queue_paused();
            if complete || user_paused {
                continue;
            }
            candidates.push(Candidate {
                hash: *entry.key(),
                session: s.clone(),
                progress: s.piece_manager.read().progress(),
            });
        }

        // Keep the almost-done torrents downloading; queue the rest.
        candidates.sort_by(|a, b| {
            b.progress
                .partial_cmp(&a.progress)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let mut active = 0usize;
        for cand in candidates {
            if active < max_dl {
                active += 1;
                if !cand.session.is_started() {
                    cand.session.resume();
                    self.spawn_start(&cand.hash, handle);
                } else if cand.session.is_queue_paused() {
                    cand.session.resume();
                }
            } else if !cand.session.is_queue_paused() {
                cand.session.set_queue_paused(true);
                cand.session.pause();
                cand.session.stats.lock().state = TorrentState::Queued;
            }
        }

        for hash in completed_but_queued {
            if let Some(s) = self.sessions.get(&hash) {
                s.resume();
            }
        }
    }

    pub fn remove_torrent(&self, info_hash: &InfoHash, delete_files: bool) {
        if let Some((_, session)) = self.sessions.remove(info_hash) {
            session.stop();
            if delete_files {
                let _ = session.storage.read().delete_files();
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

    fn load_resume_data(&self) -> Vec<InfoHash> {
        let dir = Config::resume_dir();
        let all = ResumeData::list_all(&dir);
        let mut to_start = Vec::new();
        for rd in all {
            let should_start = rd.prev_state == PrevState::Running;
            if let Some(torrent_bytes) = &rd.torrent_bytes {
                match MetaInfo::from_bytes(torrent_bytes) {
                    Ok(meta) => {
                        let info_hash = meta.info_hash;
                        match TorrentSession::new(
                            meta,
                            self.config_read().download_dir.clone(),
                            self.peer_id,
                            Arc::new(self.config_read().clone()),
                            Some(&rd.file_priorities),
                        ) {
                            Ok(session) => {
                                session.apply_resume(&rd);
                                session.set_torrent_bytes(torrent_bytes.clone());
                                self.sessions.insert(info_hash, session);
                                if should_start {
                                    to_start.push(info_hash);
                                }
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
        to_start
    }

    pub fn apply_config(&self, new_config: &Config) {
        *self.config_write() = new_config.clone();
        let new_arc = Arc::new(new_config.clone());
        for entry in self.sessions.iter() {
            let session = entry.value();
            session.set_config(new_arc.clone());
            session.dl_limiter.set_rate(new_config.max_download_rate);
            session.ul_limiter.set_rate(new_config.max_upload_rate);
        }
        self.enforce_queue_with_handle(&self.rt);
    }

    pub fn recheck_torrent(&self, info_hash: &InfoHash, rt: &tokio::runtime::Runtime) {
        if let Some(session) = self.sessions.get(info_hash) {
            let session = session.value().clone();
            rt.spawn(async move {
                session.recheck().await;
            });
        }
    }

    pub fn save_resume_data(&self, info_hash: &InfoHash) {
        let dir = Config::resume_dir();
        if let Some(session) = self.sessions.get(info_hash) {
            let rd = session.snapshot_resume();
            if let Err(e) = rd.save_to_dir(&dir) {
                tracing::warn!("Failed to save resume for {}: {}", info_hash, e);
            }
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
