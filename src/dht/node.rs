use std::cmp::Reverse;
use std::collections::VecDeque;
use std::net::{SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::{Stream, StreamExt};
use futures::stream::FuturesUnordered;
use parking_lot::RwLock;
use rand::Rng;
use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};

use crate::dht::{
    Error, Result, INACTIVITY_TIMEOUT, REQUERY_INTERVAL, RESPONSE_TIMEOUT, DHT_BOOTSTRAP,
    peer_store::PeerStore,
    protocol,
    routing_table::{InsertResult, NodeStatus, RoutingTable},
};
use crate::types::InfoHash;

fn now() -> Instant { Instant::now() }

const DHT_QUERIES_PER_SECOND: usize = 250;

struct RateLimiter {
    state: parking_lot::Mutex<RateLimiterState>,
    capacity: usize,
    refill_per_sec: usize,
}

struct RateLimiterState {
    tokens: usize,
    last_refill: Instant,
}

impl RateLimiter {
    fn new(per_sec: usize) -> Self {
        Self {
            capacity: per_sec / 10,
            refill_per_sec: per_sec,
            state: parking_lot::Mutex::new(RateLimiterState {
                tokens: per_sec / 10,
                last_refill: Instant::now(),
            }),
        }
    }

    async fn acquire(&self) {
        loop {
            {
                let mut s = self.state.lock();
                let now = Instant::now();
                let elapsed = now.duration_since(s.last_refill);
                let refill = (elapsed.as_secs_f64() * self.refill_per_sec as f64) as usize;
                if refill > 0 {
                    s.tokens = (s.tokens + refill).min(self.capacity);
                    s.last_refill = now;
                }
                if s.tokens > 0 {
                    s.tokens -= 1;
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(4)).await;
        }
    }

    fn allow(&self) -> bool {
        let mut s = self.state.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(s.last_refill);
        let refill = (elapsed.as_secs_f64() * self.refill_per_sec as f64) as usize;
        if refill > 0 {
            s.tokens = (s.tokens + refill).min(self.capacity);
            s.last_refill = now;
        }
        if s.tokens > 0 {
            s.tokens -= 1;
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DhtStats {
    pub id: String,
    pub outstanding_requests: usize,
    pub routing_table_size: usize,
    pub listen_addr: String,
}

struct OutstandingRequest {
    done: oneshot::Sender<Result<ResponseOrError>>,
}

struct WorkerSendRequest {
    our_tid: Option<u16>,
    data: Vec<u8>,
    addr: SocketAddr,
}

#[derive(Debug, Clone)]
enum Request {
    GetPeers(InfoHash),
    FindNode(InfoHash),
    Announce { info_hash: InfoHash, token: Vec<u8>, port: u16 },
    Ping,
}

enum ResponseOrError {
    Response(protocol::Message),
    Error(i32, Vec<u8>),
}

#[derive(Debug)]
struct MaybeUsefulNode {
    id: InfoHash,
    addr: SocketAddr,
    last_request: Instant,
    last_response: Option<Instant>,
    errors_in_a_row: usize,
    returned_peers: bool,
}

pub struct DhtNode {
    pub(crate) id: InfoHash,
    next_transaction_id: AtomicU16,
    inflight: DashMap<(u16, SocketAddr), OutstandingRequest>,
    pub(crate) routing_table_v4: RwLock<RoutingTable>,
    pub(crate) routing_table_v6: RwLock<RoutingTable>,
    pub(crate) listen_addr: SocketAddr,
    worker_tx: UnboundedSender<WorkerSendRequest>,
    cancellation_token: CancellationToken,
    pub(crate) peer_store: PeerStore,
    rate_limiter: RateLimiter,
    inbound_rate_limiter: RateLimiter,
}

impl DhtNode {
    fn new_internal(
        id: InfoHash,
        tx: UnboundedSender<WorkerSendRequest>,
        table_v4: Option<RoutingTable>,
        table_v6: Option<RoutingTable>,
        listen_addr: SocketAddr,
        peer_store: PeerStore,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            id,
            next_transaction_id: AtomicU16::new(0),
            inflight: DashMap::new(),
            routing_table_v4: RwLock::new(table_v4.unwrap_or_else(|| RoutingTable::new(id))),
            routing_table_v6: RwLock::new(table_v6.unwrap_or_else(|| RoutingTable::new(id))),
            listen_addr,
            worker_tx: tx,
            cancellation_token,
            peer_store,
            rate_limiter: RateLimiter::new(DHT_QUERIES_PER_SECOND),
            inbound_rate_limiter: RateLimiter::new(DHT_QUERIES_PER_SECOND * 2),
        }
    }

    fn create_message(&self, request: &Request, addr: SocketAddr) -> (u16, Vec<u8>) {
        let tid = self.next_transaction_id.fetch_add(1, Ordering::Relaxed);
        let tid_bytes = protocol::encode_transaction_id(tid);
        let want = protocol::Want::for_addr(addr);
        let msg = match request {
            Request::Ping => protocol::Message::PingRequest {
                transaction_id: tid_bytes.clone(),
                id: self.id,
            },
            Request::FindNode(target) => protocol::Message::FindNodeRequest {
                transaction_id: tid_bytes.clone(),
                id: self.id,
                target: *target,
                want,
            },
            Request::GetPeers(info_hash) => protocol::Message::GetPeersRequest {
                transaction_id: tid_bytes.clone(),
                id: self.id,
                info_hash: *info_hash,
                want,
            },
            Request::Announce { info_hash, token, port } => protocol::Message::AnnouncePeerRequest {
                transaction_id: tid_bytes.clone(),
                id: self.id,
                info_hash: *info_hash,
                token: token.clone(),
                port: *port,
                implied_port: 0,
            },
        };
        let buf = protocol::serialize(&msg);
        (tid, buf)
    }

    async fn request(&self, request: Request, addr: SocketAddr) -> Result<ResponseOrError> {
        self.rate_limiter.acquire().await;
        let (tid, data) = self.create_message(&request, addr);
        let key = (tid, addr);
        let (tx, rx) = oneshot::channel();
        self.inflight.insert(key, OutstandingRequest { done: tx });
        debug!("sending request tid={} to {}", tid, addr);
        if self.worker_tx.send(WorkerSendRequest { our_tid: Some(tid), data, addr }).is_err() {
            self.inflight.remove(&key);
            return Err(Error::DhtDead);
        }
        match tokio::time::timeout(RESPONSE_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => { self.inflight.remove(&key); Err(Error::DhtDead) }
            Err(_) => { self.inflight.remove(&key); Err(Error::ResponseTimeout(RESPONSE_TIMEOUT)) }
        }
    }

    fn table_for(&self, addr: SocketAddr) -> &RwLock<RoutingTable> {
        if addr.is_ipv4() { &self.routing_table_v4 } else { &self.routing_table_v6 }
    }

    fn generate_compact_nodes(&self, target: InfoHash, table: &RoutingTable, want: protocol::Want) -> (Vec<u8>, Vec<u8>) {
        let mut nodes_v4 = Vec::new();
        let mut nodes_v6 = Vec::new();
        for node in table.sorted_by_distance_from(target, now()).iter().take(8) {
            let node_id = node.id();
            let addr = node.addr();
            match (addr, want) {
                (SocketAddr::V4(v4), protocol::Want::V4 | protocol::Want::Both) => {
                    let mut buf = Vec::with_capacity(26);
                    buf.extend_from_slice(&node_id.0);
                    buf.extend_from_slice(&v4.ip().octets());
                    buf.extend_from_slice(&v4.port().to_be_bytes());
                    nodes_v4.extend_from_slice(&buf);
                }
                (SocketAddr::V6(v6), protocol::Want::V6 | protocol::Want::Both) => {
                    let mut buf = Vec::with_capacity(38);
                    buf.extend_from_slice(&node_id.0);
                    buf.extend_from_slice(&v6.ip().octets());
                    buf.extend_from_slice(&v6.port().to_be_bytes());
                    nodes_v6.extend_from_slice(&buf);
                }
                _ => {}
            }
        }
        (nodes_v4, nodes_v6)
    }

    fn handle_incoming(self: &Arc<Self>, msg: protocol::Message, addr: SocketAddr) -> Result<()> {
        let is_response = matches!(&msg,
            protocol::Message::Error { .. }
            | protocol::Message::PingResponse { .. }
            | protocol::Message::FindNodeResponse { .. }
            | protocol::Message::GetPeersResponse { .. }
            | protocol::Message::AnnouncePeerResponse { .. }
        );

        if is_response {
            let responder_id = match &msg {
                protocol::Message::PingResponse { id, .. }
                | protocol::Message::FindNodeResponse { id, .. }
                | protocol::Message::GetPeersResponse { id, .. }
                | protocol::Message::AnnouncePeerResponse { id, .. } => Some(*id),
                _ => None,
            };
            if let Some(rid) = responder_id {
                self.table_for(addr).write().add_node(rid, addr);
            }
            let tid = msg.transaction_id();
            let tid_val = protocol::decode_transaction_id(tid).ok_or(Error::BadTransactionId)?;
            let request = self.inflight.remove(&(tid_val, addr)).map(|(_, v)| v).ok_or(Error::RequestNotFound)?;
            let resp = match msg {
                protocol::Message::Error { code, message, .. } => ResponseOrError::Error(code, message),
                m => ResponseOrError::Response(m),
            };
            let _ = request.done.send(Ok(resp));
            return Ok(());
        }

        match &msg {
            protocol::Message::PingRequest { id, .. } => {
                self.table_for(addr).write().mark_last_query(id, now());
                let resp = protocol::serialize(&protocol::Message::PingResponse {
                    transaction_id: msg.transaction_id().to_vec(),
                    id: self.id, ip: Some(addr),
                });
                let _ = self.worker_tx.send(WorkerSendRequest { our_tid: None, data: resp, addr });
            }
            protocol::Message::FindNodeRequest { id, target, want, .. } => {
                self.table_for(addr).write().mark_last_query(id, now());
                let want = if *want == protocol::Want::None { protocol::Want::for_addr(addr) } else { *want };
                let (nodes, nodes6) = {
                    let table = self.table_for(addr).read();
                    self.generate_compact_nodes(*target, &table, want)
                };
                let resp = protocol::serialize(&protocol::Message::FindNodeResponse {
                    transaction_id: msg.transaction_id().to_vec(),
                    id: self.id, nodes, nodes6, ip: Some(addr),
                });
                let _ = self.worker_tx.send(WorkerSendRequest { our_tid: None, data: resp, addr });
            }
            protocol::Message::GetPeersRequest { id, info_hash, want, .. } => {
                self.table_for(addr).write().mark_last_query(id, now());
                let want = if *want == protocol::Want::None { protocol::Want::for_addr(addr) } else { *want };
                let (nodes, nodes6) = {
                    let table = self.table_for(addr).read();
                    self.generate_compact_nodes(*info_hash, &table, want)
                };
                let values: Vec<SocketAddr> = self.peer_store.get_peers(*info_hash).into_iter()
                    .filter(|a| match (a, want) {
                        (SocketAddr::V4(_), protocol::Want::V4 | protocol::Want::Both) => true,
                        (SocketAddr::V6(_), protocol::Want::V6 | protocol::Want::Both) => true,
                        _ => false,
                    })
                    .collect();
                let token = self.peer_store.gen_token_for(*id, addr);
                let resp = protocol::serialize(&protocol::Message::GetPeersResponse {
                    transaction_id: msg.transaction_id().to_vec(),
                    id: self.id, token: token.to_vec(),
                    values, nodes, nodes6, ip: Some(addr),
                });
                let _ = self.worker_tx.send(WorkerSendRequest { our_tid: None, data: resp, addr });
            }
            protocol::Message::AnnouncePeerRequest { id, info_hash, token, port, implied_port, .. } => {
                self.table_for(addr).write().mark_last_query(id, now());
                self.peer_store.store_peer(*id, *info_hash, token, *port, *implied_port, addr);
                let resp = protocol::serialize(&protocol::Message::AnnouncePeerResponse {
                    transaction_id: msg.transaction_id().to_vec(),
                    id: self.id, ip: Some(addr),
                });
                let _ = self.worker_tx.send(WorkerSendRequest { our_tid: None, data: resp, addr });
            }
            _ => {}
        }
        Ok(())
    }

    pub fn stats(&self) -> DhtStats {
        DhtStats {
            id: self.id.to_hex(),
            outstanding_requests: self.inflight.len(),
            routing_table_size: self.routing_table_v4.read().len(),
            listen_addr: self.listen_addr.to_string(),
        }
    }

    pub fn listen_addr(&self) -> SocketAddr { self.listen_addr }
    pub fn cancellation_token(&self) -> &CancellationToken { &self.cancellation_token }

    pub fn get_peers(self: &Arc<Self>, info_hash: InfoHash, announce_port: Option<u16>) -> RequestPeersStream {
        RequestPeersStream::new(self.clone(), info_hash, announce_port)
    }

    pub fn ping_node(self: &Arc<Self>, addr: SocketAddr) {
        let dht = self.clone();
        tokio::spawn(async move {
            let _ = dht.request(Request::Ping, addr).await;
        });
    }
}

trait RecursiveCallbacks: Sized + Send + Sync + 'static {
    fn on_start(&self, req: &RecursiveRequest<Self>, id: InfoHash, addr: SocketAddr);
    fn on_end(&self, req: &RecursiveRequest<Self>, id: InfoHash, addr: SocketAddr, resp: &Result<ResponseOrError>);
}

struct CallbacksFindNodes;
impl RecursiveCallbacks for CallbacksFindNodes {
    fn on_start(&self, req: &RecursiveRequest<Self>, id: InfoHash, addr: SocketAddr) {
        let mut rt = req.dht.table_for(addr).write();
        match rt.add_node(id, addr) {
            InsertResult::WasExisting | InsertResult::ReplacedBad(_) | InsertResult::Added => {
                rt.mark_outgoing_request(&id, now());
            }
            InsertResult::Ignored => {}
        }
    }
    fn on_end(&self, req: &RecursiveRequest<Self>, id: InfoHash, addr: SocketAddr, resp: &Result<ResponseOrError>) {
        let mut rt = req.dht.table_for(addr).write();
        if resp.is_ok() { rt.mark_response(&id, now()); } else { rt.mark_error(&id); }
    }
}

struct CallbacksGetPeers {
    announce_port: Option<u16>,
}
impl RecursiveCallbacks for CallbacksGetPeers {
    fn on_start(&self, _: &RecursiveRequest<Self>, _: InfoHash, _: SocketAddr) {}
    fn on_end(&self, req: &RecursiveRequest<Self>, target: InfoHash, addr: SocketAddr, resp: &Result<ResponseOrError>) {
        let announce_port = match self.announce_port { Some(p) => p, None => return };
        let token = match resp {
            Ok(ResponseOrError::Response(protocol::Message::GetPeersResponse { token, .. })) => token.clone(),
            _ => return,
        };
        let min_distance = InfoHash::from_hex("0000ffffffffffffffffffffffffffffffffffff").unwrap();
        if req.info_hash.distance(&target) > min_distance { return; }
        let (tid, data) = req.dht.create_message(&Request::Announce {
            info_hash: req.info_hash,
            token,
            port: announce_port,
        }, addr);
        let _ = req.dht.worker_tx.send(WorkerSendRequest { our_tid: Some(tid), data, addr });
    }
}

struct RecursiveRequest<C: RecursiveCallbacks> {
    max_depth: usize,
    useful_nodes_limit: usize,
    info_hash: InfoHash,
    request: Request,
    dht: Arc<DhtNode>,
    useful_nodes: RwLock<Vec<MaybeUsefulNode>>,
    peer_tx: UnboundedSender<SocketAddr>,
    node_tx: UnboundedSender<(Option<InfoHash>, SocketAddr, usize)>,
    callbacks: C,
}

impl<C: RecursiveCallbacks> RecursiveRequest<C> {
    async fn request_one(&self, id: Option<InfoHash>, addr: SocketAddr, depth: usize) -> Result<()> {
        if let Some(id) = id {
            self.callbacks.on_start(self, id, addr);
        }

        let response = match self.dht.request(self.request.clone(), addr).await {
            Ok(ResponseOrError::Response(r)) => r,
            Ok(ResponseOrError::Error(c, m)) => {
                debug!("error response: code={}, msg={:?}", c, m);
                self.mark_error(addr);
                return Err(Error::ErrorResponse);
            }
            Err(e) => {
                self.mark_error(addr);
                return Err(e);
            }
        };

        self.mark_responded(addr, &response);

        if let protocol::Message::GetPeersResponse { values, nodes, nodes6, .. } = &response {
            for peer in values {
                let _ = self.peer_tx.send(*peer);
            }
            for (node_id, node_addr) in protocol::parse_compact_nodes(nodes)
                .into_iter().chain(protocol::parse_compact_nodes_v6(nodes6))
            {
                if node_addr.is_ipv4() != addr.is_ipv4() { continue; }
                if self.should_request(node_id, node_addr, depth) {
                    let _ = self.node_tx.send((Some(node_id), node_addr, depth + 1));
                }
            }
        }
        if let protocol::Message::FindNodeResponse { nodes, nodes6, .. } = &response {
            for (node_id, node_addr) in protocol::parse_compact_nodes(nodes)
                .into_iter().chain(protocol::parse_compact_nodes_v6(nodes6))
            {
                if node_addr.is_ipv4() != addr.is_ipv4() { continue; }
                if self.should_request(node_id, node_addr, depth) {
                    let _ = self.node_tx.send((Some(node_id), node_addr, depth + 1));
                }
            }
        }
        if let Some(id) = id {
            self.callbacks.on_end(self, id, addr, &Ok(ResponseOrError::Response(response)));
        }
        Ok(())
    }

    fn mark_error(&self, addr: SocketAddr) {
        if let Some(n) = self.useful_nodes.write().iter_mut().find(|n| n.addr == addr) {
            n.errors_in_a_row += 1;
        }
    }

    fn mark_responded(&self, addr: SocketAddr, response: &protocol::Message) {
        if let Some(node) = self.useful_nodes.write().iter_mut().find(|n| n.addr == addr) {
            node.last_response = Some(now());
            node.errors_in_a_row = 0;
            if let protocol::Message::GetPeersResponse { values, .. } = response {
                node.returned_peers = !values.is_empty();
            }
        }
    }

    fn should_request(&self, node_id: InfoHash, addr: SocketAddr, depth: usize) -> bool {
        if depth >= self.max_depth { return false; }
        let mut nodes = self.useful_nodes.write();
        if let Some(existing) = nodes.iter_mut().find(|n| n.id == node_id) {
            if now() - existing.last_request > Duration::from_secs(60) {
                existing.last_request = now();
                return true;
            }
            return false;
        }
        nodes.push(MaybeUsefulNode {
            id: node_id, addr, last_request: now(),
            last_response: None, returned_peers: false, errors_in_a_row: 0,
        });
        nodes.sort_by_key(|n| {
            let peers_desc = Reverse(n.returned_peers);
            let resp_desc = Reverse(n.last_response.is_some() as u8);
            let dist = n.id.distance(&self.info_hash);
            let freshness = n.last_response.map(|r| now() - r).unwrap_or(Duration::MAX);
            (peers_desc, resp_desc, dist, freshness)
        });
        if nodes.len() > self.useful_nodes_limit {
            if nodes.pop().unwrap().id == node_id { return false; }
        }
        true
    }
}

impl RecursiveRequest<CallbacksFindNodes> {
    async fn find_node_for_routing_table(
        dht: Arc<DhtNode>, target: InfoHash, addrs: Vec<SocketAddr>,
    ) -> Result<()> {
        let (node_tx, mut node_rx) = unbounded_channel();
        let req = RecursiveRequest {
            max_depth: 4, info_hash: target,
            request: Request::FindNode(target),
            dht, useful_nodes_limit: 32,
            useful_nodes: RwLock::new(Vec::new()),
            peer_tx: unbounded_channel().0, node_tx,
            callbacks: CallbacksFindNodes,
        };
        let mut futs = FuturesUnordered::new();
        let mut successes = 0;
        let mut errors = 0;
        for addr in addrs {
            futs.push(req.request_one(None, addr, 0));
        }
        loop {
            tokio::select! {
                r = node_rx.recv() => {
                    if let Some((id, addr, depth)) = r {
                        futs.push(req.request_one(id, addr, depth));
                    }
                }
                f = futs.next() => {
                    match f {
                        Some(Ok(())) => successes += 1,
                        Some(Err(_)) => errors += 1,
                        None => break,
                    }
                }
            }
        }
        if successes == 0 {
            return Err(Error::NoSuccessfulLookups { errors });
        }
        Ok(())
    }
}

impl RecursiveRequest<CallbacksGetPeers> {
    fn request_peers_forever(self: Arc<Self>, is_v4: bool, mut node_rx: UnboundedReceiver<(Option<InfoHash>, SocketAddr, usize)>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let this = &*self;
            let looper = async {
                loop {
                    let sleep = match this.get_peers_root(is_v4) {
                        0 => Duration::from_secs(1),
                        n if n < 8 => REQUERY_INTERVAL / 8 * (n as u32),
                        _ => REQUERY_INTERVAL,
                    };
                    tokio::time::sleep(sleep).await;
                }
            };
            tokio::pin!(looper);
            let mut futs = FuturesUnordered::new();
            loop {
                tokio::select! {
                    biased;
                    addr = node_rx.recv() => {
                        if let Some((id, addr, depth)) = addr {
                            if is_v4 != addr.is_ipv4() { continue; }
                            futs.push(this.request_one(id, addr, depth));
                        }
                    }
                    _ = futs.next(), if !futs.is_empty() => {}
                    _ = &mut looper => {}
                }
            }
        })
    }

    fn get_peers_root(&self, is_v4: bool) -> usize {
        let probe = if is_v4 {
            SocketAddr::from(([0, 0, 0, 0], 0))
        } else {
            SocketAddr::from(([0u16; 8], 0))
        };
        let table = self.dht.table_for(probe).read();
        let count = table.sorted_by_distance_from(self.info_hash, now())
            .iter()
            .filter(|n| n.addr().is_ipv4() == is_v4)
            .take(8)
            .filter(|n| self.node_tx.send((Some(n.id()), n.addr(), 0)).is_ok())
            .count();
        count
    }
}

pub struct RequestPeersStream {
    rx: tokio::sync::mpsc::UnboundedReceiver<SocketAddr>,
    cancel_v4: tokio::task::JoinHandle<()>,
    cancel_v6: tokio::task::JoinHandle<()>,
}

impl RequestPeersStream {
    fn new(dht: Arc<DhtNode>, info_hash: InfoHash, announce_port: Option<u16>) -> Self {
        let (peer_tx, peer_rx) = unbounded_channel();
        let make = |is_v4: bool, dht: Arc<DhtNode>, peer_tx: UnboundedSender<SocketAddr>| {
            let (node_tx, node_rx) = unbounded_channel();
            let rp = Arc::new(RecursiveRequest {
                max_depth: 4, info_hash,
                useful_nodes_limit: 256,
                request: Request::GetPeers(info_hash),
                dht, useful_nodes: RwLock::new(Vec::new()),
                peer_tx, node_tx,
                callbacks: CallbacksGetPeers { announce_port },
            });
            rp.request_peers_forever(is_v4, node_rx)
        };
        let v4 = make(true, dht.clone(), peer_tx.clone());
        let v6 = make(false, dht, peer_tx);
        Self { rx: peer_rx, cancel_v4: v4, cancel_v6: v6 }
    }
}

impl Drop for RequestPeersStream {
    fn drop(&mut self) {
        self.cancel_v4.abort();
        self.cancel_v6.abort();
    }
}

impl Stream for RequestPeersStream {
    type Item = SocketAddr;
    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

struct DhtWorker {
    socket: Arc<UdpSocket>,
    dht: Arc<DhtNode>,
}

impl DhtWorker {
    async fn bootstrap_hostname(&self, hostname: &str) -> Result<()> {
        let addrs = tokio::net::lookup_host(hostname).await
            .map_err(|e| Error::BootstrapLookup { hostname: hostname.to_string(), err: e })?;
        let addrs: Vec<_> = addrs.collect();
        let v4: Vec<_> = addrs.iter().filter(|a| a.is_ipv4()).copied().collect();
        let v6: Vec<_> = addrs.iter().filter(|a| a.is_ipv6()).copied().collect();
        if !v4.is_empty() {
            let _ = RecursiveRequest::find_node_for_routing_table(self.dht.clone(), self.dht.id, v4).await;
        }
        if !v6.is_empty() {
            let _ = RecursiveRequest::find_node_for_routing_table(self.dht.clone(), self.dht.id, v6).await;
        }
        Ok(())
    }

    async fn bootstrap(&self, addrs: &[String]) -> Result<()> {
        let mut successes = 0;
        for addr in addrs {
            let mut delay_secs: u64 = 1;
            let max_delay: u64 = 60;
            loop {
                match self.bootstrap_hostname(addr).await {
                    Ok(_) => {
                        successes += 1;
                        break;
                    }
                    Err(e) => {
                        warn!("bootstrap {} failed: {}", addr, e);
                        if successes > 0 { break; }
                        let jitter = rand::random::<u64>() % 1000;
                        tokio::time::sleep(Duration::from_millis(delay_secs * 1000 + jitter)).await;
                        delay_secs = (delay_secs * 2).min(max_delay);
                    }
                }
            }
        }
        if successes == 0 { return Err(Error::BootstrapFailed); }
        Ok(())
    }

    async fn pinger(&self, is_v4: bool) -> Result<()> {
        let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT / 4);
        loop {
            interval.tick().await;
            let table = if is_v4 { &self.dht.routing_table_v4 } else { &self.dht.routing_table_v6 };
            let tn = now();
            let nodes: Vec<_> = table.read().iter_nodes()
                .filter(|n| matches!(n.status(tn), NodeStatus::Questionable | NodeStatus::Unknown))
                .map(|n| (n.id(), n.addr()))
                .collect();
            for (id, addr) in nodes {
                table.write().mark_outgoing_request(&id, tn);
                match self.dht.request(Request::Ping, addr).await {
                    Ok(_) => { table.write().mark_response(&id, tn); }
                    Err(e) => { table.write().mark_error(&id); debug!("ping error: {}", e); }
                }
            }
        }
    }

    async fn bucket_refresher(&self, is_v4: bool) -> Result<()> {
        let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT);
        interval.tick().await;
        loop {
            interval.tick().await;
            let tn = now();
            let table = if is_v4 { &self.dht.routing_table_v4 } else { &self.dht.routing_table_v6 };
            let stale: Vec<_> = table.read().iter_buckets()
                .filter(|b| tn - b.leaf.last_refreshed > INACTIVITY_TIMEOUT)
                .map(|b| b.random_id())
                .collect();
            for random_id in stale {
                let addrs: Vec<_> = table.read().sorted_by_distance_from(random_id, tn)
                    .iter().take(8).map(|n| n.addr()).collect();
                if !addrs.is_empty() {
                    let _ = RecursiveRequest::find_node_for_routing_table(
                        self.dht.clone(), random_id, addrs,
                    ).await;
                }
            }
        }
    }

    async fn framer(
        socket: Arc<UdpSocket>, dht: Arc<DhtNode>,
        mut in_rx: UnboundedReceiver<WorkerSendRequest>,
        out_tx: tokio::sync::mpsc::Sender<(protocol::Message, SocketAddr)>,
    ) -> Result<()> {
        let socket_reader = socket.clone();
        let dht_writer = dht.clone();
        let writer = async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            let mut queue: VecDeque<WorkerSendRequest> = VecDeque::new();
            loop {
                tokio::select! {
                    biased;
                    r = in_rx.recv() => {
                        match r {
                            Some(req) => queue.push_back(req),
                            None => return Err(Error::DhtDead),
                        }
                    }
                    _ = interval.tick() => {
                        if let Some(req) = queue.pop_front() {
                            if let Err(e) = socket.send_to(&req.data, req.addr).await {
                                debug!("send error to {}: {}", req.addr, e);
                                if let Some(tid) = req.our_tid {
                                    if let Some((_, req)) = dht_writer.inflight.remove(&(tid, req.addr)) {
                                        let _ = req.done.send(Err(Error::Send(e)));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        let reader = async {
            let mut buf = vec![0u8; 65536];
            loop {
                match socket_reader.recv_from(&mut buf).await {
                    Ok((size, addr)) => {
                        if size == 0 { continue; }
                        if !dht.inbound_rate_limiter.allow() {
                            debug!("inbound rate limited from {}", addr);
                            continue;
                        }
                        if let Some(msg) = protocol::deserialize(&buf[..size]) {
                            if out_tx.send((msg, addr)).await.is_err() {
                                return Err(Error::DhtDead);
                            }
                        } else {
                            trace!("unparseable KRPC from {}", addr);
                        }
                    }
                    Err(e) => {
                        warn!("recv error: {} (continuing)", e);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        };
        tokio::select! {
            e = writer => e,
            e = reader => e,
        }
    }

    async fn start(self, in_rx: UnboundedReceiver<WorkerSendRequest>, bootstrap_addrs: &[String]) -> Result<()> {
        let socket = self.socket.clone();
        let dht = self.dht.clone();
        let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<(protocol::Message, SocketAddr)>(256);
        let framer = Self::framer(socket, dht.clone(), in_rx, out_tx);
        let response_reader = async move {
            while let Some((msg, addr)) = out_rx.recv().await {
                if let Err(e) = dht.handle_incoming(msg, addr) {
                    debug!("handle_incoming error: {}", e);
                }
            }
            Err(Error::DhtDead)
        };
        tokio::pin!(framer);
        tokio::pin!(response_reader);
        let bootstrap = self.bootstrap(bootstrap_addrs);
        tokio::pin!(bootstrap);
        let pinger_v4 = self.pinger(true);
        tokio::pin!(pinger_v4);
        let refresher_v4 = self.bucket_refresher(true);
        tokio::pin!(refresher_v4);
        let pinger_v6 = self.pinger(false);
        tokio::pin!(pinger_v6);
        let refresher_v6 = self.bucket_refresher(false);
        tokio::pin!(refresher_v6);

        let mut bootstrap_done = false;
        loop {
            tokio::select! {
                e = &mut framer => return Error::task_finished("framer", e),
                r = &mut bootstrap, if !bootstrap_done => { bootstrap_done = true; r?; }
                e = &mut pinger_v4 => return Error::task_finished("pinger_v4", e),
                e = &mut refresher_v4 => return Error::task_finished("bucket_refresher_v4", e),
                e = &mut pinger_v6 => return Error::task_finished("pinger_v6", e),
                e = &mut refresher_v6 => return Error::task_finished("bucket_refresher_v6", e),
                e = &mut response_reader => return Error::task_finished("response_reader", e),
            }
        }
    }
}

impl Error {
    fn task_finished(name: &'static str, result: Result<()>) -> Result<()> {
        match result {
            Ok(()) => Err(Error::TaskQuit(name)),
            Err(e) => Err(Error::TaskFailed(name, Box::new(e))),
        }
    }
}

pub struct DhtBuilder;

impl DhtBuilder {
    pub async fn new() -> Result<Arc<DhtNode>> {
        Self::with_config(crate::dht::persistence::PersistentDhtConfig::default()).await
    }

    pub async fn with_port(port: u16) -> Result<Arc<DhtNode>> {
        let mut cfg = crate::dht::persistence::PersistentDhtConfig::default();
        cfg.port = Some(port);
        Self::with_config(cfg).await
    }

    pub async fn with_config(cfg: crate::dht::persistence::PersistentDhtConfig) -> Result<Arc<DhtNode>> {
        use crate::dht::persistence;

        let path = match cfg.config_filename.clone() {
            Some(p) => p,
            None => persistence::default_persistence_filename()?,
        };
        let dump_interval = cfg.dump_interval.unwrap_or_else(|| Duration::from_secs(60));

        let persisted = persistence::load_persistent_state(&path);

        let requested_port = cfg.port.unwrap_or(0);
        let listen_addr = SocketAddr::from(([0, 0, 0, 0], requested_port));
        let socket = Arc::new(UdpSocket::bind(listen_addr).await.map_err(Error::Bind)?);
        let actual_addr = socket.local_addr().map_err(Error::Bind)?;
        info!("DHT listening on {}", actual_addr);

        let (id, table_v4, table_v6, peer_store) = match &persisted {
            Some(p) => {
                let id = InfoHash::from_hex(&p.node_id).unwrap_or_else(|| {
                    let mut b = [0u8; 20];
                    rand::rng().fill_bytes(&mut b);
                    InfoHash(b)
                });
                let v4 = p.table_v4.clone();
                let v6 = p.table_v6.clone().unwrap_or_else(|| RoutingTable::new(id));
                let ps = persistence::make_persistent_peer_store(id, p);
                info!("DHT using persisted node ID: {}", id.to_hex());
                (id, Some(v4), Some(v6), ps)
            }
            None => {
                let mut id_bytes = [0u8; 20];
                rand::rng().fill_bytes(&mut id_bytes);
                let id = InfoHash(id_bytes);
                info!("DHT node ID (new): {}", id.to_hex());
                let ps = PeerStore::new(id);
                (id, None, None, ps)
            }
        };

        let (in_tx, in_rx) = unbounded_channel();
        let token = CancellationToken::new();
        let dht = Arc::new(DhtNode::new_internal(
            id, in_tx, table_v4, table_v6, actual_addr, peer_store, token,
        ));

        persistence::spawn_dumper(dht.clone(), path, dump_interval, dht.cancellation_token.clone());

        let worker = DhtWorker { socket, dht: dht.clone() };
        let bootstrap_addrs: Vec<String> = DHT_BOOTSTRAP.iter().map(|s| s.to_string()).collect();
        tokio::spawn(async move {
            if let Err(e) = worker.start(in_rx, &bootstrap_addrs).await {
                warn!("DHT worker stopped: {}", e);
            }
        });

        Ok(dht)
    }
}
