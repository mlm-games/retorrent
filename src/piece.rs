use crate::types::{BLOCK_SIZE, FilePriority};
use parking_lot::Mutex;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

const ISSUED_TIMEOUT_SECS: u64 = 30;

#[derive(Debug)]
pub struct PieceCollector {
    #[allow(dead_code)]
    pub index: u32,
    pub piece_size: u64,
    pub expected_hash: [u8; 20],
    pub blocks: HashMap<u32, Vec<u8>>,
    /// Offsets reserved by some peer (timestamped so a stalled peer
    /// doesn't block the offset forever).
    pub issued: HashMap<u32, Instant>,
    pub num_blocks: u32,
}

impl PieceCollector {
    pub fn new(index: u32, piece_size: u64, expected_hash: [u8; 20]) -> Self {
        let num_blocks = piece_size.div_ceil(BLOCK_SIZE as u64) as u32;
        Self {
            index,
            piece_size,
            expected_hash,
            blocks: HashMap::new(),
            issued: HashMap::new(),
            num_blocks,
        }
    }

    pub fn add_block(&mut self, offset: u32, data: Vec<u8>) -> bool {
        let was_complete = self.is_complete();
        if self.blocks.contains_key(&offset) {
            tracing::debug!(
                "piece {} dup block at offset {} ({} bytes)",
                self.index,
                offset,
                data.len()
            );
            return was_complete;
        }
        self.issued.remove(&offset);
        let total: usize = self.blocks.values().map(|v| v.len()).sum::<usize>() + data.len();
        self.blocks.insert(offset, data);
        let now_complete = self.is_complete();
        tracing::debug!(
            "piece {} block at offset {}: blocks={}/{} bytes={}/{} complete={}",
            self.index,
            offset,
            self.blocks.len(),
            self.num_blocks,
            total,
            self.piece_size,
            now_complete
        );
        if now_complete && !was_complete {
            tracing::info!(
                "piece {} assembled: blocks={}/{} bytes={}/{}",
                self.index,
                self.blocks.len(),
                self.num_blocks,
                total,
                self.piece_size
            );
        }
        now_complete
    }

    pub fn is_complete(&self) -> bool {
        self.blocks.len() >= self.num_blocks as usize
    }

    pub fn assemble(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }
        let mut result = Vec::with_capacity(self.piece_size as usize);
        let mut offsets: Vec<u32> = self.blocks.keys().cloned().collect();
        offsets.sort();
        for offset in offsets {
            result.extend_from_slice(&self.blocks[&offset]);
        }
        result.truncate(self.piece_size as usize);
        Some(result)
    }

    pub fn claim_blocks(&mut self, count: usize) -> Vec<(u32, u32)> {
        let now = Instant::now();
        let mut missing: Vec<(u32, u32)> = Vec::with_capacity(self.num_blocks as usize);
        let mut offset = 0u32;
        while (offset as u64) < self.piece_size {
            if !self.blocks.contains_key(&offset) {
                let length =
                    std::cmp::min(BLOCK_SIZE as u64, self.piece_size - offset as u64) as u32;
                missing.push((offset, length));
            }
            offset += BLOCK_SIZE;
        }
        if missing.len() > 1 {
            for i in (1..missing.len()).rev() {
                let j = (rand::random::<u64>() as usize) % (i + 1);
                missing.swap(i, j);
            }
        }
        let mut claimed = Vec::with_capacity(count.min(missing.len()));
        for (offset, length) in missing {
            if claimed.len() >= count {
                break;
            }
            // Treat issued-but-stale as unreserved.
            let is_fresh = self
                .issued
                .get(&offset)
                .map(|t| now.duration_since(*t).as_secs() < ISSUED_TIMEOUT_SECS)
                .unwrap_or(false);
            if is_fresh {
                continue;
            }
            self.issued.insert(offset, now);
            claimed.push((offset, length));
        }
        claimed
    }
}

#[derive(Debug)]
pub struct PieceManager {
    pub num_pieces: u32,
    pub piece_length: u64,
    pub total_size: u64,
    pub hashes: Vec<[u8; 20]>,
    pub have: Mutex<Vec<bool>>,
    pub in_progress: Mutex<HashMap<u32, Arc<Mutex<PieceCollector>>>>,
    pub piece_availability: Mutex<Vec<u32>>,
    piece_priorities: Mutex<Vec<FilePriority>>,
}

impl PieceManager {
    pub fn new(num_pieces: u32, piece_length: u64, total_size: u64, hashes: Vec<[u8; 20]>) -> Self {
        Self {
            num_pieces,
            piece_length,
            total_size,
            hashes,
            have: Mutex::new(vec![false; num_pieces as usize]),
            in_progress: Mutex::new(HashMap::new()),
            piece_availability: Mutex::new(vec![0; num_pieces as usize]),
            piece_priorities: Mutex::new(vec![FilePriority::Normal; num_pieces as usize]),
        }
    }

    pub fn piece_size(&self, index: u32) -> u64 {
        if index == self.num_pieces - 1 {
            let remainder = self.total_size % self.piece_length;
            if remainder == 0 {
                self.piece_length
            } else {
                remainder
            }
        } else {
            self.piece_length
        }
    }

    pub fn apply_file_priorities(
        &self,
        files: &[crate::metainfo::FileInfo],
        file_priorities: &[FilePriority],
    ) {
        let mut pp = self.piece_priorities.lock();
        for p in pp.iter_mut() {
            *p = FilePriority::Skip;
        }
        for (fi, &prio) in files.iter().zip(file_priorities.iter()) {
            if prio == FilePriority::Skip {
                continue;
            }
            let first_piece = (fi.offset / self.piece_length) as u32;
            let last_byte = fi.offset + fi.length;
            let last_piece = if last_byte == 0 {
                0
            } else {
                ((last_byte - 1) / self.piece_length) as u32
            };
            for idx in first_piece..=last_piece.min(self.num_pieces - 1) {
                let cur = pp[idx as usize];
                let chosen = match (cur, prio) {
                    (FilePriority::Skip, p) => p,
                    (FilePriority::Low, FilePriority::Normal)
                    | (FilePriority::Low, FilePriority::High) => prio,
                    (FilePriority::Normal, FilePriority::High) => prio,
                    _ => cur,
                };
                pp[idx as usize] = chosen;
            }
        }
    }

    pub fn mark_have_piece(&self, index: u32) {
        let mut avail = self.piece_availability.lock();
        if (index as usize) < avail.len() {
            avail[index as usize] += 1;
        }
    }

    pub fn select_piece(&self, peer_bitfield: &[u8]) -> Vec<u32> {
        let have = self.have.lock();
        let in_progress = self.in_progress.lock();
        let avail = self.piece_availability.lock();
        let pp = self.piece_priorities.lock();

        struct Candidate {
            index: u32,
            avail: u32,
            prio: FilePriority,
        }

        let mut candidates: Vec<Candidate> = Vec::new();

        for i in 0..self.num_pieces {
            if have[i as usize] || in_progress.contains_key(&i) {
                continue;
            }
            if pp[i as usize] == FilePriority::Skip {
                continue;
            }
            let byte_idx = (i / 8) as usize;
            let bit_offset = 7 - (i % 8);
            let peer_has =
                byte_idx < peer_bitfield.len() && (peer_bitfield[byte_idx] >> bit_offset) & 1 == 1;
            if !peer_has {
                continue;
            }
            candidates.push(Candidate {
                index: i,
                avail: avail[i as usize],
                prio: pp[i as usize],
            });
        }

        if candidates.is_empty() {
            return Vec::new();
        }

        candidates.sort_by(|a, b| {
            fn prio_rank(p: FilePriority) -> u8 {
                match p {
                    FilePriority::High => 0,
                    FilePriority::Normal => 1,
                    FilePriority::Low => 2,
                    FilePriority::Skip => 3,
                }
            }
            prio_rank(a.prio)
                .cmp(&prio_rank(b.prio))
                .then(a.avail.cmp(&b.avail))
        });

        let best_prio = candidates[0].prio;
        let best_avail = candidates[0].avail;
        candidates
            .iter()
            .filter(|c| c.prio == best_prio && c.avail <= best_avail + 2)
            .take(10)
            .map(|c| c.index)
            .collect()
    }

    pub fn is_in_endgame(&self) -> bool {
        self.progress() > 0.90
    }

    pub fn get_endgame_pieces(&self) -> Vec<u32> {
        if !self.is_in_endgame() {
            return Vec::new();
        }
        let have = self.have.lock();
        let pp = self.piece_priorities.lock();
        (0..self.num_pieces)
            .filter(|&i| !have[i as usize] && pp[i as usize] != FilePriority::Skip)
            .collect()
    }

    pub fn try_start_piece(&self, index: u32) -> Option<Arc<Mutex<PieceCollector>>> {
        let mut ip = self.in_progress.lock();
        if ip.contains_key(&index) {
            return None;
        }
        let piece_size = self.piece_size(index);
        let hash = self.hashes[index as usize];
        let collector = Arc::new(Mutex::new(PieceCollector::new(index, piece_size, hash)));
        ip.insert(index, collector.clone());
        Some(collector)
    }

    pub fn have_piece(&self, index: u32) -> bool {
        self.have
            .lock()
            .get(index as usize)
            .copied()
            .unwrap_or(false)
    }

    pub fn piece_in_progress(&self, index: u32) -> bool {
        self.in_progress.lock().contains_key(&index)
    }

    pub fn piece_is_skipped(&self, index: u32) -> bool {
        let pp = self.piece_priorities.lock();
        (index as usize) < pp.len() && pp[index as usize] == FilePriority::Skip
    }

    pub fn mark_piece_complete(&self, index: u32) {
        self.in_progress.lock().remove(&index);
        self.have.lock()[index as usize] = true;
        let mut avail = self.piece_availability.lock();
        if (index as usize) < avail.len() {
            avail[index as usize] += 1;
        }
    }

    pub fn force_start_piece(&self, index: u32) -> Arc<Mutex<PieceCollector>> {
        let mut ip = self.in_progress.lock();
        if let Some(existing) = ip.get(&index) {
            return existing.clone();
        }
        let piece_size = self.piece_size(index);
        let hash = self.hashes[index as usize];
        let collector = Arc::new(Mutex::new(PieceCollector::new(index, piece_size, hash)));
        ip.insert(index, collector.clone());
        collector
    }

    /// Pick an in-progress piece that the peer has, preferring ones
    /// closest to completion so we can finish a piece quickly. Returns
    /// (index, collector) or None if no in-progress piece matches the
    /// peer's bitfield.
    pub fn pick_in_progress_for_peer(
        &self,
        peer_bitfield: &[u8],
    ) -> Option<(u32, Arc<Mutex<PieceCollector>>)> {
        let in_progress = self.in_progress.lock();
        let mut best: Option<(u32, Arc<Mutex<PieceCollector>>, u32)> = None;
        for (idx, collector) in in_progress.iter() {
            let byte_idx = (*idx / 8) as usize;
            let bit_offset = 7 - (*idx % 8);
            let peer_has =
                byte_idx < peer_bitfield.len() && (peer_bitfield[byte_idx] >> bit_offset) & 1 == 1;
            if !peer_has {
                continue;
            }
            let filled = collector.lock().blocks.len() as u32;
            // Skip pieces already complete — they should be in `have`
            // and removed from in_progress, but guard anyway.
            if filled >= collector.lock().num_blocks {
                continue;
            }
            if best.as_ref().map(|b| filled > b.2).unwrap_or(true) {
                best = Some((*idx, collector.clone(), filled));
            }
        }
        best.map(|(i, c, _)| (i, c))
    }

    pub fn abort_piece(&self, index: u32) {
        self.in_progress.lock().remove(&index);
    }

    pub fn completed_count(&self) -> u32 {
        self.have.lock().iter().filter(|&&h| h).count() as u32
    }

    pub fn progress(&self) -> f32 {
        if self.num_pieces == 0 {
            return 0.0;
        }
        self.completed_count() as f32 / self.num_pieces as f32
    }

    pub fn is_complete(&self) -> bool {
        self.completed_count() == self.num_pieces
    }

    pub fn have_bitfield(&self) -> Vec<u8> {
        let have = self.have.lock();
        let byte_count = (self.num_pieces as usize).div_ceil(8);
        let mut bitfield = vec![0u8; byte_count];
        for (i, &has) in have.iter().enumerate() {
            if has {
                let byte_idx = i / 8;
                let bit_offset = 7 - (i % 8);
                bitfield[byte_idx] |= 1 << bit_offset;
            }
        }
        bitfield
    }

    pub fn get_have_vec(&self) -> Vec<bool> {
        self.have.lock().clone()
    }

    pub fn load_have(&self, bits: &[bool]) {
        let mut have = self.have.lock();
        for (i, &b) in bits.iter().enumerate() {
            if i < have.len() {
                have[i] = b;
            }
        }
    }
}
