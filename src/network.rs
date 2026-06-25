use crate::config::Config;
use crate::dht::DhtNode;
use crate::error::Result;
use crate::metainfo::MetaInfo;
use crate::peer::{MetadataState, PeerConnection, PeerMessage};
use crate::piece::PieceManager;
use crate::storage::DiskStorage;
use crate::tracker::TrackerClient;
use crate::types::*;
use futures::StreamExt;
use parking_lot::Mutex;
use parking_lot::RwLock;
use sha1::Digest;
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};

enum PeerEvent {
    AddPeers(Vec<SocketAddrV4>),
    IncomingConnection(TcpStream, SocketAddrV4),
}

#[derive(Debug, Clone, Copy)]
pub struct PeerStats {
    pub downloaded: u64,
    pub am_choking: bool,
    pub peer_interested: bool,
}

#[derive(Debug, Clone)]
pub struct TorrentStats {
    pub download_rate: u64,
    pub upload_rate: u64,
    pub downloaded: u64,
    pub uploaded: u64,
    pub connected_peers: usize,
    pub seeders: u64,
    pub leechers: u64,
    pub state: TorrentState,
    pub progress: f32,
    pub eta_seconds: Option<u64>,
}

pub struct TorrentSession {
    pub info_hash: InfoHash,
    pub peer_id: PeerId,
    pub meta: Arc<RwLock<MetaInfo>>,
    pub piece_manager: Arc<RwLock<Arc<PieceManager>>>,
    pub storage: Arc<RwLock<Arc<DiskStorage>>>,
    pub stats: Arc<Mutex<TorrentStats>>,
    pub paused: Arc<AtomicBool>,
    started: AtomicBool,
    pub total_downloaded: Arc<AtomicU64>,
    pub total_uploaded: Arc<AtomicU64>,
    pub active_peers: Arc<AtomicU64>,
    pub file_priorities: Arc<Mutex<Vec<FilePriority>>>,
    cancel_token: parking_lot::Mutex<tokio_util::sync::CancellationToken>,
    completed_sent: Arc<AtomicBool>,
    seed_ratio_reached: Arc<AtomicBool>,
    completed_time: Mutex<Option<i64>>,
    pub dl_limiter: Arc<RateLimiter>,
    pub ul_limiter: Arc<RateLimiter>,
    config: Arc<Config>,
    pub torrent_bytes: Arc<Mutex<Option<Vec<u8>>>>,
    pub peer_choke_stats: Arc<Mutex<HashMap<SocketAddrV4, PeerStats>>>,
    dht: parking_lot::Mutex<Option<Arc<DhtNode>>>,
    /// State for in-progress ut_metadata exchange (magnet links).
    pub metadata_state: Arc<Mutex<Option<super::peer::MetadataState>>>,
}

impl TorrentSession {
    pub fn new(
        meta: MetaInfo,
        download_dir: std::path::PathBuf,
        peer_id: PeerId,
        config: Arc<Config>,
        initial_priorities: Option<&[FilePriority]>,
    ) -> Result<Arc<Self>> {
        let piece_manager = Arc::new(PieceManager::new(
            meta.num_pieces(),
            meta.piece_length,
            meta.total_size,
            meta.pieces.clone(),
        ));

        let fp_vec = if let Some(p) = initial_priorities {
            p.to_vec()
        } else {
            vec![FilePriority::Normal; meta.files.len()]
        };

        let file_priorities = Arc::new(Mutex::new(fp_vec));

        let storage = Arc::new(DiskStorage::new(
            download_dir,
            &meta,
            config.prealloc_files,
            config.cache_size_mb,
            file_priorities.clone(),
        )?);

        // Reflect initial priorities in the piece manager.
        {
            let fp = file_priorities.lock();
            piece_manager.apply_file_priorities(&meta.files, &fp);
        }

        let meta = Arc::new(RwLock::new(meta));
        let piece_manager = Arc::new(RwLock::new(piece_manager));
        let storage = Arc::new(RwLock::new(storage));

        let stats = Arc::new(Mutex::new(TorrentStats {
            download_rate: 0,
            upload_rate: 0,
            downloaded: 0,
            uploaded: 0,
            connected_peers: 0,
            seeders: 0,
            leechers: 0,
            state: TorrentState::Downloading,
            progress: 0.0,
            eta_seconds: None,
        }));

        let info_hash = meta.read().info_hash;

        Ok(Arc::new(Self {
            info_hash,
            peer_id,
            meta,
            piece_manager,
            storage,
            stats,
            paused: Arc::new(AtomicBool::new(false)),
            started: AtomicBool::new(false),
            total_downloaded: Arc::new(AtomicU64::new(0)),
            total_uploaded: Arc::new(AtomicU64::new(0)),
            active_peers: Arc::new(AtomicU64::new(0)),
            file_priorities,
            cancel_token: parking_lot::Mutex::new(tokio_util::sync::CancellationToken::new()),
            completed_sent: Arc::new(AtomicBool::new(false)),
            seed_ratio_reached: Arc::new(AtomicBool::new(false)),
            completed_time: Mutex::new(None),
            dl_limiter: Arc::new(RateLimiter::new(config.max_download_rate)),
            ul_limiter: Arc::new(RateLimiter::new(config.max_upload_rate)),
            config,
            torrent_bytes: Arc::new(Mutex::new(None)),
            peer_choke_stats: Arc::new(Mutex::new(HashMap::new())),
            dht: parking_lot::Mutex::new(None),
            metadata_state: Arc::new(Mutex::new(None)),
        }))
    }

    pub fn set_torrent_bytes(&self, data: Vec<u8>) {
        self.torrent_bytes.lock().replace(data);
    }

    pub fn apply_resume(&self, rd: &ResumeData) {
        self.piece_manager.read().load_have(&rd.have_pieces);
        self.total_downloaded
            .store(rd.downloaded, Ordering::Relaxed);
        self.total_uploaded.store(rd.uploaded, Ordering::Relaxed);
        if rd.file_priorities.len() == self.meta.read().files.len() {
            *self.file_priorities.lock() = rd.file_priorities.clone();
            self.piece_manager
                .read()
                .apply_file_priorities(&self.meta.read().files, &rd.file_priorities);
        }
        self.paused
            .store(rd.prev_state == PrevState::Paused, Ordering::Relaxed);

        let state = if rd.prev_state == PrevState::Paused {
            TorrentState::Paused
        } else if self.piece_manager.read().is_complete() {
            TorrentState::Seeding
        } else {
            TorrentState::Downloading
        };
        let progress = self.piece_manager.read().progress();
        let mut stats = self.stats.lock();
        stats.state = state;
        stats.progress = progress;
        stats.downloaded = rd.downloaded;
        stats.uploaded = rd.uploaded;
    }

    pub fn snapshot_resume(&self) -> ResumeData {
        if self.piece_manager.read().is_complete() {
            let mut ct = self.completed_time.lock();
            if ct.is_none() {
                *ct = Some(chrono::Utc::now().timestamp());
            }
        }
        ResumeData {
            info_hash: self.info_hash.to_hex(),
            have_pieces: self.piece_manager.read().get_have_vec(),
            downloaded: self.total_downloaded.load(Ordering::Relaxed),
            uploaded: self.total_uploaded.load(Ordering::Relaxed),
            file_priorities: self.file_priorities.lock().clone(),
            added_time: chrono::Utc::now().timestamp(),
            completed_time: *self.completed_time.lock(),
            torrent_bytes: self.torrent_bytes.lock().clone(),
            prev_state: if self.paused.load(Ordering::Relaxed)
                || !self.started.load(Ordering::Relaxed)
            {
                PrevState::Paused
            } else {
                PrevState::Running
            },
        }
    }

    pub fn set_file_priority(&self, file_index: usize, priority: FilePriority) {
        let mut fp = self.file_priorities.lock();
        if file_index < fp.len() {
            fp[file_index] = priority;
            self.piece_manager
                .read()
                .apply_file_priorities(&self.meta.read().files, &fp);
        }
    }

    pub fn get_file_priorities(&self) -> Vec<FilePriority> {
        self.file_priorities.lock().clone()
    }

    pub async fn start(self: Arc<Self>, max_peers: usize, dht: Option<Arc<DhtNode>>) {
        if self.started.swap(true, Ordering::Relaxed) {
            tracing::warn!("start() called twice for {}", self.info_hash);
            return;
        }

        // Store DHT reference for Port message handling
        *self.dht.lock() = dht.clone();

        let (peer_tx, mut peer_rx) = mpsc::channel::<PeerEvent>(100);

        let tracker_session = self.clone();
        let peer_tx2 = peer_tx.clone();
        tokio::spawn(async move {
            tracker_session.tracker_loop(peer_tx2).await;
        });

        if let Some(ref dht_node) = dht {
            let dht_session = self.clone();
            let dht_peer_tx = peer_tx.clone();
            let dht = dht_node.clone();
            let info_hash = self.info_hash;
            let port = self.config.listen_port;
            tokio::spawn(async move {
                dht_session
                    .dht_loop(dht, info_hash, port, dht_peer_tx)
                    .await;
            });
        }

        if self.config.accept_incoming {
            let listener_session = self.clone();
            let listener_tx = peer_tx.clone();
            tokio::spawn(async move {
                listener_session.incoming_listener(listener_tx).await;
            });
        }

        let semaphore = Arc::new(Semaphore::new(max_peers));
        let peer_session = self.clone();
        let session_tx = peer_tx.clone();
        tokio::spawn(async move {
            while let Some(event) = peer_rx.recv().await {
                if peer_session.seed_ratio_reached.load(Ordering::Relaxed) {
                    continue;
                }
                let (addr, incoming) = match event {
                    PeerEvent::AddPeers(peers) => {
                        for addr in peers {
                            if peer_session.paused.load(Ordering::Relaxed) {
                                continue;
                            }
                            let session = peer_session.clone();
                            let sem = semaphore.clone();
                            let evt_tx = session_tx.clone();
                            tokio::spawn(async move {
                                let _permit = match sem.acquire().await {
                                    Ok(p) => p,
                                    Err(_) => return,
                                };
                                struct ActiveGuard<'a> {
                                    counter: &'a AtomicU64,
                                }
                                impl Drop for ActiveGuard<'_> {
                                    fn drop(&mut self) {
                                        self.counter.fetch_sub(1, Ordering::Relaxed);
                                    }
                                }
                                session.active_peers.fetch_add(1, Ordering::Relaxed);
                                let _guard = ActiveGuard {
                                    counter: &session.active_peers,
                                };

                                if let Err(e) = session.handle_peer(addr, None, evt_tx).await {
                                    tracing::debug!("Peer {} error: {}", addr, e);
                                }
                            });
                        }
                        continue;
                    }
                    PeerEvent::IncomingConnection(stream, addr) => (addr, Some(stream)),
                };

                if peer_session.paused.load(Ordering::Relaxed) {
                    continue;
                }
                let session = peer_session.clone();
                let sem = semaphore.clone();
                let evt_tx = session_tx.clone();
                tokio::spawn(async move {
                    let _permit = match sem.acquire().await {
                        Ok(p) => p,
                        Err(_) => return,
                    };
                    struct ActiveGuard<'a> {
                        counter: &'a AtomicU64,
                    }
                    impl Drop for ActiveGuard<'_> {
                        fn drop(&mut self) {
                            self.counter.fetch_sub(1, Ordering::Relaxed);
                        }
                    }
                    session.active_peers.fetch_add(1, Ordering::Relaxed);
                    let _guard = ActiveGuard {
                        counter: &session.active_peers,
                    };

                    if let Err(e) = session.handle_peer(addr, incoming, evt_tx).await {
                        tracing::debug!("Peer {} error: {}", addr, e);
                    }
                });
            }
        });

        let stats_session = self.clone();
        tokio::spawn(async move {
            stats_session.stats_loop().await;
        });

        if self.config.webseed_enabled
            && !self.meta.read().url_list.is_empty()
            && !self.meta.read().is_private
        {
            let webseed_source = crate::webseed::WebseedSource {
                meta: Arc::new(self.meta.read().clone()),
                piece_manager: self.piece_manager.read().clone(),
                storage: self.storage.read().clone(),
                stats: self.stats.clone(),
                total_downloaded: self.total_downloaded.clone(),
                cancel: self.cancel_token.lock().clone(),
            };
            tokio::spawn(async move {
                webseed_source.run().await;
            });
        }

        let choke_session = self.clone();
        tokio::spawn(async move {
            choke_session.choke_loop().await;
        });
    }

    async fn dht_loop(
        self: Arc<Self>,
        dht: Arc<DhtNode>,
        info_hash: InfoHash,
        port: u16,
        peer_tx: mpsc::Sender<PeerEvent>,
    ) {
        let mut peers = dht.get_peers(info_hash, Some(port));
        let token = self.cancel_token.lock().clone();
        loop {
            tokio::select! {
                Some(peer) = peers.next() => {
                    if let SocketAddr::V4(v4) = peer {
                        if token.is_cancelled() { break; }
                        let _ = peer_tx.send(PeerEvent::AddPeers(vec![v4])).await;
                    }
                }
                _ = token.cancelled() => break,
            }
        }
    }

    async fn incoming_listener(&self, peer_tx: mpsc::Sender<PeerEvent>) {
        let bind_addr = format!("0.0.0.0:{}", self.config.listen_port);
        let listener = match TcpListener::bind(&bind_addr).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!("Failed to bind listener on {}: {}", bind_addr, e);
                return;
            }
        };
        tracing::info!("Listening for incoming peers on {}", bind_addr);

        let token = self.cancel_token.lock().clone();
        loop {
            if token.is_cancelled() {
                break;
            }
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, remote)) => {
                            if let Ok(v4) = Self::to_v4(remote) {
                                tracing::debug!("Incoming connection from {}", v4);
                                let _ = peer_tx.send(PeerEvent::IncomingConnection(stream, v4)).await;
                            }
                        }
                        Err(e) => {
                            tracing::debug!("Accept error: {}", e);
                        }
                    }
                }
                _ = token.cancelled() => break,
            }
        }
    }

    fn to_v4(addr: std::net::SocketAddr) -> std::result::Result<SocketAddrV4, ()> {
        match addr {
            std::net::SocketAddr::V4(v4) => Ok(v4),
            _ => Err(()),
        }
    }

    async fn tracker_loop(&self, peer_tx: mpsc::Sender<PeerEvent>) {
        let client = TrackerClient::new();
        let mut interval = 30u64;
        let mut first_announce = true;

        let mut trackers: Vec<String> = Vec::new();
        if let Some(ref announce) = self.meta.read().announce {
            trackers.push(announce.clone());
        }
        for tier in &self.meta.read().announce_list {
            for url in tier {
                if !trackers.contains(url) {
                    trackers.push(url.clone());
                }
            }
        }

        let token = self.cancel_token.lock().clone();

        loop {
            if token.is_cancelled() {
                break;
            }

            let total_size = self.meta.read().total_size;
            let progress = self.piece_manager.read().progress();
            let left = (total_size as f64 * (1.0f64 - progress as f64)) as u64;

            for tracker_url in &trackers {
                let event = if first_announce {
                    first_announce = false;
                    Some("started")
                } else {
                    None
                };
                match client
                    .announce(
                        tracker_url,
                        &self.info_hash,
                        &self.peer_id,
                        self.config.listen_port,
                        self.total_uploaded.load(Ordering::Relaxed),
                        self.total_downloaded.load(Ordering::Relaxed),
                        left,
                        event,
                    )
                    .await
                {
                    Ok(response) => {
                        interval = response.interval.max(response.min_interval.unwrap_or(0));
                        {
                            let mut stats = self.stats.lock();
                            if let Some(s) = response.seeders {
                                stats.seeders = s;
                            }
                            if let Some(l) = response.leechers {
                                stats.leechers = l;
                            }
                        }
                        if !response.peers.is_empty() {
                            let _ = peer_tx.send(PeerEvent::AddPeers(response.peers)).await;
                        }
                        tracing::info!("Tracker {} OK, interval={}s", tracker_url, interval);
                        break;
                    }
                    Err(e) => {
                        tracing::warn!("Tracker {} error: {}", tracker_url, e);
                    }
                }
            }

            if self.piece_manager.read().is_complete()
                && !self.completed_sent.swap(true, Ordering::Relaxed)
                && let Some(tracker_url) = trackers.first()
            {
                let _ = client
                    .announce(
                        tracker_url,
                        &self.info_hash,
                        &self.peer_id,
                        self.config.listen_port,
                        self.total_uploaded.load(Ordering::Relaxed),
                        self.total_downloaded.load(Ordering::Relaxed),
                        0,
                        Some("completed"),
                    )
                    .await;
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(interval)) => {},
                _ = token.cancelled() => break,
            }
        }

        if let Some(tracker_url) = trackers.first() {
            let _ = client
                .announce(
                    tracker_url,
                    &self.info_hash,
                    &self.peer_id,
                    self.config.listen_port,
                    self.total_uploaded.load(Ordering::Relaxed),
                    self.total_downloaded.load(Ordering::Relaxed),
                    0,
                    Some("stopped"),
                )
                .await;
        }
    }

    async fn handle_peer(
        &self,
        addr: SocketAddrV4,
        incoming: Option<TcpStream>,
        peer_event_tx: mpsc::Sender<PeerEvent>,
    ) -> Result<()> {
        let mut conn = if let Some(stream) = incoming {
            PeerConnection::accept(stream, addr, &self.info_hash, &self.peer_id).await?
        } else {
            PeerConnection::connect(addr, &self.info_hash, &self.peer_id).await?
        };

        if self.meta.read().num_pieces() > 0 {
            let bitfield = self.piece_manager.read().have_bitfield();
            conn.send_message(&PeerMessage::Bitfield(bitfield)).await?;
        }

        conn.send_message(&PeerMessage::Interested).await?;
        conn.am_interested = true;

        // BEP-10 extended handshake. Must be sent before any other extended
        // message. We advertise ut_pex (id=1).
        let meta_size = self.torrent_bytes.lock().as_ref().map(|b| b.len());
        let hs = PeerMessage::build_extended_handshake_payload(
            self.config.pipeline_depth as u32,
            meta_size,
        );
        conn.send_message(&PeerMessage::Extended { id: 0, payload: hs })
            .await?;

        self.peer_choke_stats.lock().insert(
            addr,
            PeerStats {
                downloaded: 0,
                am_choking: true,
                peer_interested: false,
            },
        );

        let is_metadata_mode = self.meta.read().num_pieces() == 0;
        if is_metadata_mode {
            self.metadata_state
                .lock()
                .get_or_insert_with(|| MetadataState::new(self.info_hash));
        }

        let mut total_sent: u32 = 0;
        let mut current_piece: Option<Arc<parking_lot::Mutex<crate::piece::PieceCollector>>> = None;
        let mut current_piece_index: Option<u32> = None;
        let mut pending_requests: u32 = 0;
        let mut blocks_in_piece: u32 = 0;
        let max_pipeline = self.config.pipeline_depth;
        let mut issued_by_us: HashSet<u32> = HashSet::new();

        let mut keepalive_timer = Instant::now();
        let mut last_pex_send = Instant::now();
        let mut last_pex_drain = Instant::now();
        let mut known_peers: Vec<SocketAddrV4> = Vec::new();
        let mut already_dialed: HashSet<SocketAddrV4> = HashSet::new();
        let token = self.cancel_token.lock().clone();

        loop {
            if token.is_cancelled() || self.paused.load(Ordering::Relaxed) {
                break;
            }
            if self.meta.read().num_pieces() > 0 && self.piece_manager.read().is_complete() {
                self.stats.lock().state = TorrentState::Seeding;
                if self.config.seed_ratio_enabled {
                    let dl = self.total_downloaded.load(Ordering::Relaxed);
                    let ul = self.total_uploaded.load(Ordering::Relaxed);
                    if dl > 0 && (ul as f64 / dl as f64) >= self.config.seed_ratio_limit {
                        tracing::info!("Seed ratio reached, stopping");
                        self.seed_ratio_reached.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }

            {
                let should_choke = self
                    .peer_choke_stats
                    .lock()
                    .get(&conn.addr)
                    .map(|s| s.am_choking)
                    .unwrap_or(true);
                if should_choke != conn.am_choking {
                    if should_choke {
                        conn.send_message(&PeerMessage::Choke).await?;
                        conn.am_choking = true;
                        pending_requests = 0;
                    } else {
                        conn.send_message(&PeerMessage::Unchoke).await?;
                        conn.am_choking = false;
                    }
                }
            }

            if keepalive_timer.elapsed() > Duration::from_secs(120) {
                conn.send_message(&PeerMessage::KeepAlive).await?;
                keepalive_timer = Instant::now();
            }

            if !conn.peer_choking && pending_requests < max_pipeline {
                if current_piece.is_none() {
                    let in_endgame =
                        self.config.endgame_mode && self.piece_manager.read().is_in_endgame();
                    let candidates = if in_endgame {
                        self.piece_manager.read().get_endgame_pieces()
                    } else {
                        self.piece_manager.read().select_piece(&conn.bitfield)
                    };

                    for piece_idx in candidates.iter().take(10) {
                        if let Some(collector) =
                            self.piece_manager.read().try_start_piece(*piece_idx)
                        {
                            blocks_in_piece = collector.lock().num_blocks;
                            total_sent = 0;
                            current_piece = Some(collector);
                            current_piece_index = Some(*piece_idx);
                            break;
                        } else if in_endgame {
                            let collector = self.piece_manager.read().force_start_piece(*piece_idx);
                            blocks_in_piece = collector.lock().num_blocks;
                            total_sent = 0;
                            current_piece = Some(collector);
                            current_piece_index = Some(*piece_idx);
                            break;
                        }
                    }

                    // No new piece could be started — every candidate is
                    // already in_progress (started by some other peer but
                    // not yet complete). Join an existing in-progress
                    // piece so we can fill the last few blocks. Without
                    // this, pieces stall at 400+ blocks once all
                    // candidates are taken: nobody requests the missing
                    // blocks because try_start_piece returns None.
                    if current_piece.is_none() {
                        if let Some((idx, collector)) = self
                            .piece_manager
                            .read()
                            .pick_in_progress_for_peer(&conn.bitfield)
                        {
                            blocks_in_piece = collector.lock().num_blocks;
                            total_sent = 0;
                            current_piece = Some(collector);
                            current_piece_index = Some(idx);
                        }
                    }
                }

                if let Some(ref collector) = current_piece {
                    let max_to_send = blocks_in_piece.saturating_sub(total_sent);
                    if max_to_send > 0 {
                        let pipeline_room = max_pipeline.saturating_sub(pending_requests);
                        let want = (max_to_send as usize).min(pipeline_room as usize);
                        // `claim_blocks` reserves the offsets in the
                        // piece's issued set (with a 30s timeout) and
                        // returns them in a randomized order so each
                        // peer's 32-block pipeline covers a different
                        // slice of the piece.
                        let claimed = collector.lock().claim_blocks(want);
                        for (offset, length) in claimed {
                            self.dl_limiter.wait_consume(length as u64).await;
                            conn.send_message(&PeerMessage::Request {
                                index: current_piece_index.unwrap(),
                                begin: offset,
                                length,
                            })
                            .await?;
                            pending_requests += 1;
                            total_sent += 1;
                            issued_by_us.insert(offset);
                        }
                    }
                }
            }

            if self.config.pex_enabled
                && last_pex_send.elapsed() > Duration::from_secs(10)
                && !known_peers.is_empty()
            {
                let new_peers: Vec<_> = known_peers
                    .iter()
                    .filter(|p| **p != conn.addr)
                    .take(50)
                    .cloned()
                    .collect();
                if !new_peers.is_empty() {
                    let pex_payload = PeerMessage::build_pex_payload(&new_peers, &[]);
                    conn.send_message(&PeerMessage::Extended {
                        id: 1,
                        payload: pex_payload,
                    })
                    .await?;
                }
                last_pex_send = Instant::now();
            }

            // Periodically flush PEX-discovered peers into the connection
            // pipeline. Without this, retorrent learns about hundreds of
            // peers via PEX and never actually connects to any of them —
            // a much bigger hit than it sounds, because qBittorrent uses
            // PEX peers as its primary source after the first handshake.
            if self.config.pex_enabled
                && last_pex_drain.elapsed() > Duration::from_secs(15)
                && !known_peers.is_empty()
            {
                let to_dial: Vec<SocketAddrV4> = known_peers
                    .iter()
                    .filter(|p| **p != conn.addr && !already_dialed.contains(*p))
                    .take(20)
                    .cloned()
                    .collect();
                for p in &to_dial {
                    already_dialed.insert(*p);
                }
                if !to_dial.is_empty() {
                    let _ = peer_event_tx.send(PeerEvent::AddPeers(to_dial)).await;
                }
                last_pex_drain = Instant::now();
            }

            let msg = match tokio::time::timeout(Duration::from_secs(10), conn.recv_message()).await
            {
                Ok(Ok(msg)) => msg,
                Ok(Err(e)) => {
                    tracing::debug!("Peer {} recv error: {}", addr, e);
                    break;
                }
                Err(_) => {
                    continue;
                }
            };

            match msg {
                PeerMessage::KeepAlive => {}
                PeerMessage::Choke => {
                    conn.peer_choking = true;
                    pending_requests = 0;
                }
                PeerMessage::Unchoke => {
                    conn.peer_choking = false;
                }
                PeerMessage::Interested => {
                    conn.peer_interested = true;
                    self.peer_choke_stats
                        .lock()
                        .get_mut(&conn.addr)
                        .map(|s| s.peer_interested = true);
                }
                PeerMessage::NotInterested => {
                    conn.peer_interested = false;
                    self.peer_choke_stats
                        .lock()
                        .get_mut(&conn.addr)
                        .map(|s| s.peer_interested = false);
                }
                PeerMessage::Have(piece) => {
                    if piece >= self.piece_manager.read().num_pieces {
                        continue;
                    }
                    if !conn.has_piece(piece) {
                        conn.set_piece(piece);
                        self.piece_manager.read().mark_have_piece(piece);
                    }
                }
                PeerMessage::Bitfield(bf) => {
                    let expected = self.piece_manager.read().num_pieces.div_ceil(8) as usize;
                    if bf.len() != expected {
                        continue;
                    }
                    for i in 0..self.piece_manager.read().num_pieces {
                        let byte_idx = (i / 8) as usize;
                        let bit_offset = 7 - (i % 8);
                        let has_bit = byte_idx < bf.len() && (bf[byte_idx] >> bit_offset) & 1 == 1;
                        if has_bit && !conn.has_piece(i) {
                            self.piece_manager.read().mark_have_piece(i);
                        }
                    }
                    conn.bitfield = bf.clone();
                }
                PeerMessage::Extended { id: 0, payload } => {
                    let (pex_id, metadata_id, metadata_size) =
                        PeerMessage::parse_extended_handshake(&payload);
                    if let Some(pex_id) = pex_id {
                        conn.peer_pex_id = Some(pex_id);
                    }
                    if let Some(metadata_id) = metadata_id {
                        conn.peer_metadata_id = Some(metadata_id);
                        if self.meta.read().num_pieces() == 0 {
                            tracing::trace!(
                                "Peer {} supports ut_metadata (id={}), metadata_size={:?}",
                                conn.addr,
                                metadata_id,
                                metadata_size
                            );
                            // Don't ask if peer is in back-off from a prior reject.
                            let in_backoff = conn
                                .metadata_reject_until
                                .map_or(false, |t| t > std::time::Instant::now());
                            if !in_backoff {
                                // Pre-allocate metadata buffer from peer's advertised size.
                                if let Some(size) = metadata_size {
                                    let mut md = self.metadata_state.lock();
                                    if let Some(ref mut ms) = *md {
                                        if ms.total_size == 0 {
                                            ms.set_total_size(size);
                                        }
                                    }
                                }
                                let request_piece = {
                                    let md = self.metadata_state.lock();
                                    md.as_ref()
                                        .and_then(|ms| ms.pieces.iter().position(|p| p.is_none()))
                                        .unwrap_or(0)
                                };
                                tracing::trace!(
                                    "Requesting metadata piece {} from {}",
                                    request_piece,
                                    conn.addr
                                );
                                let req = PeerMessage::build_metadata_request(request_piece);
                                conn.send_message(&PeerMessage::Extended {
                                    id: metadata_id,
                                    payload: req,
                                })
                                .await?;
                                conn.metadata_sent_requests.push(request_piece);
                            } else {
                                tracing::trace!(
                                    "Peer {} in metadata backoff until {:?}",
                                    conn.addr,
                                    conn.metadata_reject_until
                                );
                            }
                        }
                    }
                }
                PeerMessage::Extended { id, payload } => {
                    let pex_id = conn.peer_pex_id.unwrap_or(1);
                    let metadata_id = conn.peer_metadata_id;

                    if id == pex_id {
                        let (added, _dropped) = PeerMessage::parse_pex_payload(&payload);
                        for peer in added {
                            if peer != conn.addr && !known_peers.contains(&peer) {
                                known_peers.push(peer);
                            }
                        }
                    } else if Some(id) == metadata_id {
                        let (msg_type, piece, total_size, data) =
                            PeerMessage::parse_metadata_message(&payload);
                        match msg_type {
                            0 => {
                                // Peer requests metadata from us.
                                let tbytes = self.torrent_bytes.lock().clone();
                                if let Some(ref tbytes) = tbytes {
                                    if let Ok(info_raw) = crate::bencode::extract_info_raw(tbytes) {
                                        let chunks: Vec<&[u8]> =
                                            info_raw.chunks(BLOCK_SIZE as usize).collect();
                                        if piece < chunks.len() {
                                            let chunk = chunks[piece];
                                            let total_size = if piece == 0 {
                                                Some(info_raw.len())
                                            } else {
                                                None
                                            };
                                            // Rate-limit responses to avoid send-buffer bloat.
                                            let can_respond =
                                                conn.metadata_last_response.map_or(true, |t| {
                                                    t.elapsed()
                                                        > std::time::Duration::from_millis(100)
                                                });
                                            if can_respond {
                                                let resp = PeerMessage::build_metadata_data(
                                                    piece, total_size, chunk,
                                                );
                                                conn.send_message(&PeerMessage::Extended {
                                                    id,
                                                    payload: resp,
                                                })
                                                .await?;
                                                conn.metadata_last_response =
                                                    Some(std::time::Instant::now());
                                            } else {
                                                let reject =
                                                    PeerMessage::build_metadata_reject(piece);
                                                conn.send_message(&PeerMessage::Extended {
                                                    id,
                                                    payload: reject,
                                                })
                                                .await?;
                                            }
                                        } else {
                                            let reject = PeerMessage::build_metadata_reject(piece);
                                            conn.send_message(&PeerMessage::Extended {
                                                id,
                                                payload: reject,
                                            })
                                            .await?;
                                        }
                                    }
                                } else {
                                    let reject = PeerMessage::build_metadata_reject(piece);
                                    conn.send_message(&PeerMessage::Extended {
                                        id,
                                        payload: reject,
                                    })
                                    .await?;
                                }
                            }
                            1 => {
                                // Data message: store metadata piece.
                                // Dedup guard: only accept data for pieces we requested.
                                let idx =
                                    conn.metadata_sent_requests.iter().position(|p| *p == piece);
                                if idx.is_none() {
                                    tracing::debug!(
                                        "Ignoring unsolicited metadata piece {} from {}",
                                        piece,
                                        conn.addr
                                    );
                                    continue;
                                }
                                conn.metadata_sent_requests.swap_remove(idx.unwrap());

                                let (need_next, maybe_assembled) = {
                                    let mut md_guard = self.metadata_state.lock();
                                    if let Some(ref mut ms) = *md_guard {
                                        if piece == 0 && total_size > 0 {
                                            ms.set_total_size(total_size);
                                        }
                                        if piece == 0 && ms.total_size == 0 {
                                            (None, None)
                                        } else {
                                            tracing::trace!(
                                                "Stored metadata piece {} from {} (total_size={}, {}/{} pieces)",
                                                piece,
                                                conn.addr,
                                                ms.total_size,
                                                ms.pieces.iter().filter(|p| p.is_some()).count(),
                                                ms.pieces.len()
                                            );
                                            let done = ms.store_piece(piece, data);
                                            if done {
                                                if let Some(assembled) = ms.assemble() {
                                                    if ms.verify(&assembled) {
                                                        *md_guard = None;
                                                        (None, Some(assembled))
                                                    } else {
                                                        tracing::warn!(
                                                            "Metadata hash mismatch for {}",
                                                            self.info_hash
                                                        );
                                                        *md_guard = Some(MetadataState::new(
                                                            self.info_hash,
                                                        ));
                                                        (None, None)
                                                    }
                                                } else {
                                                    (None, None)
                                                }
                                            } else {
                                                let next = piece + 1;
                                                if next < ms.num_pieces {
                                                    (Some((next, id)), None)
                                                } else {
                                                    (None, None)
                                                }
                                            }
                                        }
                                    } else {
                                        (None, None)
                                    }
                                };

                                if let Some(assembled) = maybe_assembled {
                                    if let Err(e) = self.finalize_metadata(assembled).await {
                                        tracing::warn!("Failed to finalize metadata: {}", e);
                                    }
                                    break;
                                }
                                if let Some((next, ext_id)) = need_next {
                                    let in_backoff = conn
                                        .metadata_reject_until
                                        .map_or(false, |t| t > std::time::Instant::now());
                                    if !in_backoff {
                                        tracing::trace!(
                                            "Requesting metadata piece {} from {}",
                                            next,
                                            conn.addr
                                        );
                                        let req = PeerMessage::build_metadata_request(next);
                                        conn.send_message(&PeerMessage::Extended {
                                            id: ext_id,
                                            payload: req,
                                        })
                                        .await?;
                                        conn.metadata_sent_requests.push(next);
                                    }
                                }
                            }
                            2 => {
                                // Reject: peer doesn't have this piece.
                                conn.metadata_sent_requests.retain(|p| *p != piece);
                                conn.metadata_reject_until = Some(
                                    std::time::Instant::now() + std::time::Duration::from_secs(60),
                                );
                                tracing::debug!(
                                    "Peer {} rejected metadata piece {}",
                                    conn.addr,
                                    piece
                                );
                            }
                            _ => {}
                        }
                    } else {
                        continue;
                    }
                }
                PeerMessage::Piece { index, begin, data } => {
                    pending_requests = pending_requests.saturating_sub(1);
                    issued_by_us.remove(&begin);
                    let data_len = data.len() as u64;
                    self.total_downloaded.fetch_add(data_len, Ordering::Relaxed);
                    self.peer_choke_stats
                        .lock()
                        .get_mut(&conn.addr)
                        .map(|s| s.downloaded = s.downloaded.saturating_add(data_len));

                    if current_piece_index != Some(index) {
                        continue;
                    }

                    if let Some(ref collector) = current_piece {
                        let complete = collector.lock().add_block(begin, data);
                        if complete {
                            let captured = {
                                let c = collector.lock();
                                c.assemble().map(|data| (data, c.expected_hash))
                            };
                            if let Some((piece_data, expected_hash)) = captured {
                                let storage = self.storage.read().clone();
                                let pm = self.piece_manager.read().clone();
                                let idx = index;
                                tracing::info!(
                                    "verifying piece {} ({} bytes)",
                                    idx,
                                    piece_data.len()
                                );
                                tokio::spawn(async move {
                                    if let Err(e) = tokio::task::spawn_blocking(move || {
                                        let mut hasher = sha1::Sha1::new();
                                        sha1::Digest::update(&mut hasher, &piece_data);
                                        let result = hasher.finalize();
                                        if result.as_slice() == expected_hash {
                                            if let Err(e) = storage.write_piece(idx, &piece_data) {
                                                tracing::warn!("Write piece {}: {}", idx, e);
                                                pm.abort_piece(idx);
                                                return;
                                            }
                                            pm.mark_piece_complete(idx);
                                            tracing::info!(
                                                "Piece {} OK ({:.1}%)",
                                                idx,
                                                pm.progress() * 100.0
                                            );
                                            tracing::debug!("piece {} verified and written", idx);
                                        } else {
                                            let already_done = pm
                                                .get_have_vec()
                                                .get(idx as usize)
                                                .copied()
                                                .unwrap_or(false);
                                            if !already_done {
                                                tracing::warn!(
                                                    "Piece {} hash mismatch, retrying",
                                                    idx
                                                );
                                                pm.abort_piece(idx);
                                            }
                                        }
                                    })
                                    .await
                                    {
                                        tracing::error!(
                                            "Piece verification panicked for {}: {}",
                                            idx,
                                            e
                                        );
                                    }
                                });
                            }
                            current_piece = None;
                            current_piece_index = None;
                            pending_requests = 0;
                            total_sent = 0;
                            issued_by_us.clear();
                        }
                    }
                }
                PeerMessage::Request {
                    index,
                    begin,
                    length,
                } => {
                    if !conn.am_choking {
                        if index >= self.piece_manager.read().num_pieces
                            || length == 0
                            || length > BLOCK_SIZE * 2
                        {
                            continue;
                        }
                        if self.piece_manager.read().have_piece(index) {
                            let piece_size = self.piece_manager.read().piece_size(index);
                            if begin as u64 + length as u64 > piece_size {
                                continue;
                            }
                            let read_result = self.storage.read().read_piece(index, piece_size);
                            if let Ok(piece_data) = read_result {
                                let block = piece_data
                                    [begin as usize..begin as usize + length as usize]
                                    .to_vec();

                                self.ul_limiter.wait_consume(block.len() as u64).await;

                                self.total_uploaded
                                    .fetch_add(block.len() as u64, Ordering::Relaxed);
                                conn.send_message(&PeerMessage::Piece {
                                    index,
                                    begin,
                                    data: block,
                                })
                                .await?;
                            }
                        }
                    }
                }
                PeerMessage::Cancel { .. } => {}
                PeerMessage::Port(dht_port) => {
                    if let Some(ref dht) = *self.dht.lock() {
                        let dht_addr = SocketAddr::V4(SocketAddrV4::new(*addr.ip(), dht_port));
                        if dht_addr.port() != 0 {
                            dht.ping_node(dht_addr);
                        }
                    }
                }
                _ => {}
            }
        }

        if let Some(idx) = current_piece_index {
            // Release any blocks this conn claimed but never received.
            // The piece's 30s timeout would also free these, but
            // releasing on exit lets another peer claim them
            // immediately instead of waiting up to 30s.
            if let Some(collector) = current_piece.as_ref() {
                let mut c = collector.lock();
                for offset in &issued_by_us {
                    c.issued.remove(offset);
                }
            }
            self.piece_manager.read().abort_piece(idx);
            pending_requests = 0;
        }

        self.peer_choke_stats.lock().remove(&addr);

        Ok(())
    }

    async fn choke_loop(self: Arc<Self>) {
        let mut prev_downloaded: HashMap<SocketAddrV4, u64> = HashMap::new();
        let mut last_opt_unchoke = Instant::now();
        let token = self.cancel_token.lock().clone();

        loop {
            if token.is_cancelled() {
                break;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;

            if token.is_cancelled() {
                break;
            }

            let (current, interested_set) = {
                let stats = self.peer_choke_stats.lock();
                let current: HashMap<SocketAddrV4, u64> =
                    stats.iter().map(|(a, s)| (*a, s.downloaded)).collect();
                let interested: Vec<SocketAddrV4> = stats
                    .iter()
                    .filter(|(_, s)| s.peer_interested)
                    .map(|(a, _)| *a)
                    .collect();
                (current, interested)
            };

            let now = Instant::now();

            let mut speeds: Vec<(SocketAddrV4, u64)> = current
                .iter()
                .map(|(a, &dl)| {
                    let prev = prev_downloaded.get(a).copied().unwrap_or(0);
                    (*a, dl.saturating_sub(prev))
                })
                .filter(|(a, _)| interested_set.contains(a))
                .collect();

            speeds.sort_by(|a, b| b.1.cmp(&a.1));

            let mut to_unchoke: Vec<SocketAddrV4> = Vec::new();
            let slots = self.config.upload_slots.max(1);

            for (addr, _) in speeds.iter().take(slots.saturating_sub(1)) {
                to_unchoke.push(*addr);
            }

            if last_opt_unchoke.elapsed()
                >= Duration::from_secs(self.config.optimistic_unchoke_interval)
            {
                let candidates: Vec<SocketAddrV4> = interested_set
                    .iter()
                    .filter(|a| !to_unchoke.contains(a))
                    .copied()
                    .collect();
                if !candidates.is_empty() {
                    let idx = rand::random_range(0..candidates.len());
                    to_unchoke.push(candidates[idx]);
                }
                last_opt_unchoke = now;
            }

            if to_unchoke.is_empty() && !speeds.is_empty() {
                to_unchoke.push(speeds[0].0);
            }

            {
                let mut stats = self.peer_choke_stats.lock();
                for (addr, state) in stats.iter_mut() {
                    state.am_choking = !to_unchoke.contains(addr);
                }
            }

            prev_downloaded = current;
        }
    }

    async fn stats_loop(&self) {
        let mut last_downloaded = 0u64;
        let mut last_uploaded = 0u64;
        let mut resume_timer = Instant::now();
        let token = self.cancel_token.lock().clone();

        loop {
            if token.is_cancelled() {
                break;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;

            let downloaded = self.total_downloaded.load(Ordering::Relaxed);
            let uploaded = self.total_uploaded.load(Ordering::Relaxed);
            let dl_rate = downloaded.saturating_sub(last_downloaded);
            let ul_rate = uploaded.saturating_sub(last_uploaded);
            last_downloaded = downloaded;
            last_uploaded = uploaded;

            let progress = self.piece_manager.read().progress();
            let remaining =
                (self.meta.read().total_size as f64 * (1.0f64 - progress as f64)) as u64;
            let eta = if dl_rate > 0 {
                Some(remaining / dl_rate)
            } else {
                None
            };

            let state = if self.paused.load(Ordering::Relaxed) {
                TorrentState::Paused
            } else if self.meta.read().num_pieces() == 0 {
                TorrentState::FetchingMetadata
            } else if self.piece_manager.read().is_complete()
                && self.seed_ratio_reached.load(Ordering::Relaxed)
            {
                TorrentState::Complete
            } else if self.piece_manager.read().is_complete() {
                TorrentState::Seeding
            } else {
                TorrentState::Downloading
            };

            {
                let mut s = self.stats.lock();
                s.download_rate = dl_rate;
                s.upload_rate = ul_rate;
                s.downloaded = downloaded;
                s.uploaded = uploaded;
                s.connected_peers = self.active_peers.load(Ordering::Relaxed) as usize;
                s.progress = progress;
                s.state = state;
                s.eta_seconds = eta;
            }

            if resume_timer.elapsed() > Duration::from_secs(30) {
                let rd = self.snapshot_resume();
                let dir = Config::resume_dir();
                if let Err(e) = rd.save_to_dir(&dir) {
                    tracing::warn!("Failed to save resume data: {}", e);
                }
                resume_timer = Instant::now();
            }
        }
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
        self.cancel_token.lock().cancel();
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        *self.cancel_token.lock() = tokio_util::sync::CancellationToken::new();
        self.started.store(false, Ordering::Relaxed);
    }

    pub fn stop(&self) {
        self.cancel_token.lock().cancel();
        let rd = self.snapshot_resume();
        let _ = rd.save_to_dir(&Config::resume_dir());
    }

    pub fn get_stats(&self) -> TorrentStats {
        self.stats.lock().clone()
    }

    /// Wrap a raw info-dict in a minimal torrent file.
    fn wrap_info_dict(info_dict: &[u8]) -> Vec<u8> {
        let mut data = b"d4:info".to_vec();
        data.extend_from_slice(info_dict);
        data.push(b'e');
        data
    }

    /// Called when all metadata pieces have been received and verified.
    /// Reconstructs the session with the real MetaInfo, PieceManager, and
    /// DiskStorage.
    async fn finalize_metadata(&self, assembled: Vec<u8>) -> Result<()> {
        if self.meta.read().num_pieces() > 0 {
            // Another peer already completed the metadata exchange.
            return Ok(());
        }

        let torrent_bytes = Self::wrap_info_dict(&assembled);
        let real_meta = MetaInfo::from_bytes(&torrent_bytes)?;

        // Reset file priorities before creating DiskStorage so it sees
        // the correct priority vector length.
        {
            let mut fp = self.file_priorities.lock();
            *fp = vec![FilePriority::Normal; real_meta.files.len()];
        }

        let new_pm = Arc::new(PieceManager::new(
            real_meta.num_pieces(),
            real_meta.piece_length,
            real_meta.total_size,
            real_meta.pieces.clone(),
        ));
        new_pm.apply_file_priorities(&real_meta.files, &self.file_priorities.lock());

        let new_storage = Arc::new(DiskStorage::new(
            self.storage.read().base_path().to_path_buf(),
            &real_meta,
            self.config.prealloc_files,
            self.config.cache_size_mb,
            self.file_priorities.clone(),
        )?);

        *self.meta.write() = real_meta;
        *self.piece_manager.write() = new_pm;
        *self.storage.write() = new_storage;
        self.set_torrent_bytes(torrent_bytes);
        self.stats.lock().state = TorrentState::Downloading;

        tracing::info!("Session updated with metadata for {}", self.info_hash);
        Ok(())
    }
}
