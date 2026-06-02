use std::net::SocketAddr;
use std::time::Instant;

use rand::Rng;
use serde::{Deserialize, Deserializer, Serialize, Serializer, ser::SerializeStruct};
use tracing::{debug, trace};

use crate::types::InfoHash;
use crate::dht::INACTIVITY_TIMEOUT;

const BUCKET_MAX_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct LeafBucket {
    pub nodes: Vec<RoutingTableNode>,
    pub last_refreshed: Instant,
}

impl Serialize for LeafBucket {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut s = serializer.serialize_struct("LeafBucket", 2)?;
        s.serialize_field("nodes", &self.nodes)?;
        s.serialize_field("last_refreshed", &format!("{:?}", self.last_refreshed.elapsed()))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for LeafBucket {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        struct Tmp { nodes: Vec<RoutingTableNode> }
        Tmp::deserialize(deserializer).map(|t| Self {
            nodes: t.nodes,
            last_refreshed: Instant::now(),
        })
    }
}

impl Default for LeafBucket {
    fn default() -> Self {
        Self { nodes: Default::default(), last_refreshed: Instant::now() }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum BucketTreeNodeData {
    Leaf(LeafBucket),
    LeftRight(usize, usize),
}

#[derive(Debug, Clone)]
struct BucketTreeNode {
    bits: u8,
    start: InfoHash,
    end_inclusive: InfoHash,
    data: BucketTreeNodeData,
}

impl Serialize for BucketTreeNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("BucketTreeNode", 4)?;
        s.serialize_field("bits", &self.bits)?;
        s.serialize_field("start", &self.start.to_hex())?;
        s.serialize_field("end_inclusive", &self.end_inclusive.to_hex())?;
        s.serialize_field("data", &self.data)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for BucketTreeNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        struct Tmp {
            bits: u8,
            start: String,
            end_inclusive: String,
            data: BucketTreeNodeData,
        }
        Tmp::deserialize(deserializer).and_then(|t| Ok(Self {
            bits: t.bits,
            start: InfoHash::from_hex(&t.start).ok_or_else(|| serde::de::Error::custom("bad hex in start"))?,
            end_inclusive: InfoHash::from_hex(&t.end_inclusive).ok_or_else(|| serde::de::Error::custom("bad hex in end_inclusive"))?,
            data: t.data,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct BucketTree {
    data: Vec<BucketTreeNode>,
    size: usize,
    max_size: usize,
}

impl Serialize for BucketTree {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut s = serializer.serialize_struct("BucketTree", 3)?;
        s.serialize_field("data", &self.data)?;
        s.serialize_field("size", &self.size)?;
        s.serialize_field("max_size", &self.max_size)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for BucketTree {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        struct Tmp {
            data: Vec<BucketTreeNode>,
            size: usize,
            max_size: usize,
        }
        Tmp::deserialize(deserializer).map(|t| Self {
            data: t.data,
            size: t.size,
            max_size: t.max_size,
        })
    }
}

pub struct BucketTreeIteratorItem<'a> {
    pub bits: u8,
    pub start: &'a InfoHash,
    pub end_inclusive: &'a InfoHash,
    pub leaf: &'a LeafBucket,
}

impl BucketTreeIteratorItem<'_> {
    pub fn random_id(&self) -> InfoHash {
        generate_random_id(self.start, self.bits)
    }
}

struct BucketTreeIterator<'a> {
    tree: &'a BucketTree,
    queue: Vec<usize>,
}

impl<'a> BucketTreeIterator<'a> {
    fn new(tree: &'a BucketTree) -> Self {
        Self { tree, queue: vec![0] }
    }
}

impl<'a> Iterator for BucketTreeIterator<'a> {
    type Item = BucketTreeIteratorItem<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let idx = self.queue.pop()?;
            match self.tree.data.get(idx) {
                Some(node) => match &node.data {
                    BucketTreeNodeData::Leaf(leaf) => {
                        return Some(BucketTreeIteratorItem {
                            bits: node.bits, start: &node.start,
                            end_inclusive: &node.end_inclusive, leaf,
                        });
                    }
                    BucketTreeNodeData::LeftRight(left, right) => {
                        self.queue.push(*right);
                        self.queue.push(*left);
                        continue;
                    }
                },
                None => continue,
            }
        }
    }
}

pub fn generate_random_id(start: &InfoHash, bits: u8) -> InfoHash {
    let mut data = [0u8; 20];
    rand::rng().fill_bytes(&mut data);
    let mut id = InfoHash(data);
    let remaining_bits = 160 - bits;
    for bit in 0..remaining_bits {
        id.set_bit(bit, start.get_bit(bit));
    }
    id
}

fn compute_split(start: InfoHash, end: InfoHash, bits: u8) -> ((InfoHash, InfoHash), (InfoHash, InfoHash)) {
    let changing_bit = 160 - bits;
    let left_end = {
        let mut c = end;
        c.set_bit(changing_bit, false);
        c
    };
    let right_start = {
        let mut c = start;
        c.set_bit(changing_bit, true);
        c
    };
    ((start, left_end), (right_start, end))
}

#[derive(Debug)]
pub enum InsertResult {
    WasExisting,
    ReplacedBad(RoutingTableNode),
    Added,
    Ignored,
}

impl BucketTree {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: vec![BucketTreeNode {
                bits: 160,
                start: InfoHash([0u8; 20]),
                end_inclusive: InfoHash([0xff; 20]),
                data: BucketTreeNodeData::Leaf(Default::default()),
            }],
            size: 0,
            max_size,
        }
    }

    fn iter_leaves(&self) -> BucketTreeIterator<'_> {
        BucketTreeIterator::new(self)
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = &'_ RoutingTableNode> + '_ {
        self.iter_leaves().flat_map(|l| l.leaf.nodes.iter())
    }

    fn get_leaf(&self, id: &InfoHash) -> usize {
        let mut idx = 0;
        loop {
            let node = &self.data[idx];
            match node.data {
                BucketTreeNodeData::Leaf(_) => return idx,
                BucketTreeNodeData::LeftRight(left, right) => {
                    let lnode = &self.data[left];
                    if *id >= lnode.start && *id <= lnode.end_inclusive {
                        idx = left;
                    } else {
                        idx = right;
                    }
                }
            }
        }
    }

    pub fn get_mut(&mut self, id: &InfoHash, refresh: Option<Instant>) -> Option<&mut RoutingTableNode> {
        let idx = self.get_leaf(id);
        match &mut self.data[idx].data {
            BucketTreeNodeData::Leaf(leaf) => {
                let r = leaf.nodes.iter_mut().find(|n| n.id == *id);
                if r.is_some() {
                    if let Some(t) = refresh { leaf.last_refreshed = t; }
                }
                r
            }
            BucketTreeNodeData::LeftRight(_, _) => unreachable!(),
        }
    }

    pub fn add_node(&mut self, self_id: &InfoHash, id: InfoHash, addr: SocketAddr) -> InsertResult {
        let idx = self.get_leaf(&id);
        self.insert_into_leaf(idx, self_id, id, addr)
    }

    fn insert_into_leaf(&mut self, mut idx: usize, self_id: &InfoHash, id: InfoHash, addr: SocketAddr) -> InsertResult {
        let now = Instant::now();
        loop {
            let leaf = &mut self.data[idx];
            let nodes = match &mut leaf.data {
                BucketTreeNodeData::Leaf(n) => n,
                BucketTreeNodeData::LeftRight(_, _) => unreachable!(),
            };

            if nodes.nodes.iter().any(|n| n.id == id) {
                return InsertResult::WasExisting;
            }

            let mut new_node = RoutingTableNode {
                id, addr, last_request: None, last_response: None,
                last_query: None, errors_in_a_row: 0,
            };

            if let Some(bad) = nodes.nodes.iter_mut().find(|n| n.status(now) == NodeStatus::Bad) {
                std::mem::swap(bad, &mut new_node);
                nodes.nodes.sort_by_key(|n| n.id);
                nodes.last_refreshed = now;
                return InsertResult::ReplacedBad(new_node);
            }

            if self.size >= self.max_size {
                return InsertResult::Ignored;
            }

            if nodes.nodes.len() < BUCKET_MAX_SIZE {
                nodes.nodes.push(new_node);
                nodes.nodes.sort_by_key(|n| n.id);
                nodes.last_refreshed = now;
                self.size += 1;
                return InsertResult::Added;
            }

            if *self_id < leaf.start || *self_id > leaf.end_inclusive {
                return InsertResult::Ignored;
            }

            let ((ls, le), (rs, re)) = compute_split(leaf.start, leaf.end_inclusive, leaf.bits);
            let (mut ld, mut rd) = (Vec::new(), Vec::new());
            for node in nodes.nodes.drain(..) {
                if node.id < rs { ld.push(node); } else { rd.push(node); }
            }

            let left = BucketTreeNode {
                bits: leaf.bits - 1, start: ls, end_inclusive: le,
                data: BucketTreeNodeData::Leaf(LeafBucket { nodes: ld, ..Default::default() }),
            };
            let right = BucketTreeNode {
                bits: leaf.bits - 1, start: rs, end_inclusive: re,
                data: BucketTreeNodeData::Leaf(LeafBucket { nodes: rd, ..Default::default() }),
            };

            let left_idx = { let l = self.data.len(); self.data.push(left); l };
            let right_idx = { let l = self.data.len(); self.data.push(right); l };
            self.data[idx].data = BucketTreeNodeData::LeftRight(left_idx, right_idx);

            idx = if id < rs { left_idx } else { right_idx };
        }
    }
}

#[derive(Debug, Clone)]
pub struct RoutingTableNode {
    pub id: InfoHash,
    pub addr: SocketAddr,
    last_request: Option<Instant>,
    last_response: Option<Instant>,
    last_query: Option<Instant>,
    pub errors_in_a_row: usize,
}

impl Serialize for RoutingTableNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut s = serializer.serialize_struct("RoutingTableNode", 4)?;
        s.serialize_field("id", &self.id.to_hex())?;
        s.serialize_field("addr", &self.addr)?;
        s.serialize_field("status", &self.status(Instant::now()))?;
        s.serialize_field("errors_in_a_row", &self.errors_in_a_row)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for RoutingTableNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        struct Tmp {
            id: String,
            addr: SocketAddr,
            #[allow(dead_code)]
            status: Option<NodeStatus>,
            #[serde(default)]
            errors_in_a_row: usize,
        }
        let t = Tmp::deserialize(deserializer)?;
        let id = InfoHash::from_hex(&t.id).ok_or_else(|| serde::de::Error::custom("bad hex in id"))?;
        Ok(Self {
            id,
            addr: t.addr,
            last_request: None,
            last_response: None,
            last_query: None,
            errors_in_a_row: t.errors_in_a_row,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeStatus {
    Good, Questionable, Bad, Unknown,
}

impl RoutingTableNode {
    pub fn id(&self) -> InfoHash { self.id }
    pub fn addr(&self) -> SocketAddr { self.addr }

    pub fn status(&self, now: Instant) -> NodeStatus {
        match (self.last_request, self.last_response, self.last_query) {
            (Some(_), _, _) if self.errors_in_a_row >= 2 => NodeStatus::Bad,
            (Some(_), Some(last), _) | (Some(_), _, Some(last))
                if now - last < INACTIVITY_TIMEOUT => NodeStatus::Good,
            (last_out, _, Some(last_in)) | (last_out, Some(last_in), _)
                if now - last_in > INACTIVITY_TIMEOUT
                    && last_out.map(|e| now - e > INACTIVITY_TIMEOUT).unwrap_or(true) =>
                NodeStatus::Questionable,
            _ => NodeStatus::Unknown,
        }
    }

    pub fn mark_outgoing_request(&mut self, now: Instant) { self.last_request = Some(now); }
    pub fn mark_last_query(&mut self, now: Instant) { self.last_query = Some(now); }
    pub fn mark_response(&mut self, now: Instant) {
        self.last_response = Some(now);
        if self.last_request.is_none() { self.last_request = Some(now); }
        self.errors_in_a_row = 0;
    }
    pub fn mark_error(&mut self) { self.errors_in_a_row += 1; }
}

#[derive(Debug, Clone)]
pub struct RoutingTable {
    id: InfoHash,
    size: usize,
    buckets: BucketTree,
}

impl Serialize for RoutingTable {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        let mut s = serializer.serialize_struct("RoutingTable", 3)?;
        s.serialize_field("id", &self.id.to_hex())?;
        s.serialize_field("size", &self.size)?;
        s.serialize_field("buckets", &self.buckets)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for RoutingTable {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        struct Tmp {
            id: String,
            #[allow(dead_code)]
            size: usize,
            buckets: BucketTree,
        }
        let t = Tmp::deserialize(deserializer)?;
        let id = InfoHash::from_hex(&t.id).ok_or_else(|| serde::de::Error::custom("bad hex in id"))?;
        let size = t.buckets.iter_nodes().count();
        Ok(Self { id, size, buckets: t.buckets })
    }
}

impl RoutingTable {
    const DEFAULT_MAX_SIZE: usize = 512;

    pub fn new(id: InfoHash) -> Self {
        Self {
            id,
            buckets: BucketTree::new(Self::DEFAULT_MAX_SIZE),
            size: 0,
        }
    }

    pub fn id(&self) -> InfoHash { self.id }
    pub fn len(&self) -> usize { self.size }

    pub fn sorted_by_distance_from(&self, target: InfoHash, now: Instant) -> Vec<&RoutingTableNode> {
        let mut result: Vec<_> = self.buckets.iter_nodes().collect();
        result.sort_by_key(|n| {
            let status_ord = match n.status(now) {
                NodeStatus::Good => 0,
                NodeStatus::Questionable => 1,
                NodeStatus::Unknown => 2,
                NodeStatus::Bad => 3,
            };
            (status_ord, target.distance(&n.id))
        });
        result
    }

    /// Returns up to `k` closest good-or-questionable node addresses to `target`.
    pub fn closest_nodes(&self, target: InfoHash, k: usize) -> Vec<SocketAddr> {
        self.sorted_by_distance_from(target, Instant::now())
            .into_iter()
            .filter(|n| !matches!(n.status(Instant::now()), NodeStatus::Bad))
            .map(|n| n.addr)
            .take(k)
            .collect()
    }

    pub fn iter_buckets(&self) -> impl Iterator<Item = BucketTreeIteratorItem<'_>> + '_ {
        self.buckets.iter_leaves()
    }

    pub fn iter_nodes(&self) -> impl Iterator<Item = &'_ RoutingTableNode> + '_ {
        self.buckets.iter_nodes()
    }

    pub fn add_node(&mut self, id: InfoHash, addr: SocketAddr) -> InsertResult {
        let result = self.buckets.add_node(&self.id, id, addr);
        match &result {
            InsertResult::WasExisting => {}
            InsertResult::ReplacedBad(_) | InsertResult::Added => { self.size += 1; }
            InsertResult::Ignored => {}
        }
        result
    }

    pub fn mark_outgoing_request(&mut self, id: &InfoHash, now: Instant) -> bool {
        self.buckets.get_mut(id, None).map(|n| { n.mark_outgoing_request(now); true }).unwrap_or(false)
    }

    pub fn mark_response(&mut self, id: &InfoHash, now: Instant) -> bool {
        self.buckets.get_mut(id, Some(now)).map(|n| { n.mark_response(now); true }).unwrap_or(false)
    }

    pub fn mark_error(&mut self, id: &InfoHash) -> bool {
        self.buckets.get_mut(id, None).map(|n| { n.mark_error(); true }).unwrap_or(false)
    }

    pub fn mark_last_query(&mut self, id: &InfoHash, now: Instant) -> bool {
        self.buckets.get_mut(id, None).map(|n| { n.mark_last_query(now); true }).unwrap_or(false)
    }
}
