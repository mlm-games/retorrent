use crate::error::{Result, TorrentError};
use crate::metainfo::MetaInfo;
use crate::types::FilePriority;
use lru::LruCache;
use memmap2::MmapOptions;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct DiskStorage {
    base_path: PathBuf,
    meta: MetaInfo,
    read_cache: Mutex<LruCache<u32, Arc<Vec<u8>>>>,
    write_files: Mutex<HashMap<PathBuf, File>>,
    file_priorities: Arc<Mutex<Vec<FilePriority>>>,
}

impl DiskStorage {
    pub fn new(
        base_path: PathBuf,
        meta: &MetaInfo,
        prealloc_files: bool,
        cache_size_mb: usize,
        file_priorities: Arc<Mutex<Vec<FilePriority>>>,
    ) -> Result<Self> {
        let priorities_guard = file_priorities.lock();
        for (idx, file_info) in meta.files.iter().enumerate() {
            let is_skipped = idx < priorities_guard.len()
                && priorities_guard[idx] == FilePriority::Skip;
            if is_skipped {
                // Create parent dirs so sibling files still work, but skip the file itself.
                let full_path = base_path.join(&file_info.path);
                if let Some(parent) = full_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                continue;
            }
            let full_path = base_path.join(&file_info.path);
            if let Some(parent) = full_path.parent() {
                fs::create_dir_all(parent)?;
            }
            if !full_path.exists() {
                let f = File::create(&full_path)?;
                if prealloc_files {
                    f.set_len(file_info.length)?;
                }
            }
        }
        drop(priorities_guard);

        let cache_entries = if meta.piece_length > 0 {
            let max_bytes = (cache_size_mb as u64).saturating_mul(1024 * 1024);
            let entries = max_bytes / meta.piece_length;
            entries.max(64).min(2048) as usize
        } else {
            128
        };

        Ok(Self {
            base_path,
            meta: meta.clone(),
            read_cache: Mutex::new(LruCache::new(
                NonZeroUsize::new(cache_entries.max(1)).unwrap(),
            )),
            write_files: Mutex::new(HashMap::new()),
            file_priorities,
        })
    }

    fn get_write_file(&self, rel_path: &str) -> Result<File> {
        let full_path = self.base_path.join(rel_path);
        if let Some(f) = self.write_files.lock().get(&full_path) {
            return Ok(f.try_clone()?);
        }
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&full_path)
            .map_err(|e| TorrentError::Storage(e.to_string()))?;
        let cloned = f.try_clone()?;
        self.write_files.lock().insert(full_path, f);
        Ok(cloned)
    }

    fn open_file_read(&self, path: &str) -> Result<File> {
        let full_path = self.base_path.join(path);
        OpenOptions::new()
            .read(true)
            .open(&full_path)
            .map_err(|e| TorrentError::Storage(e.to_string()))
    }

    fn open_file_write_uncached(&self, path: &str) -> Result<File> {
        let full_path = self.base_path.join(path);
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&full_path)
            .map_err(|e| TorrentError::Storage(e.to_string()))
    }

    pub fn write_piece(&self, index: u32, data: &[u8]) -> Result<()> {
        let piece_offset = index as u64 * self.meta.piece_length;
        let mut data_offset = 0usize;
        let mut remaining = data.len();

        let priorities = self.file_priorities.lock().clone();
        for (file_idx, file_info) in self.meta.files.iter().enumerate() {
            let file_start = file_info.offset;
            let file_end = file_info.offset + file_info.length;

            let abs_start = piece_offset + data_offset as u64;
            if abs_start >= file_end || abs_start + remaining as u64 <= file_start {
                continue;
            }

            let offset_in_file = abs_start.saturating_sub(file_start);
            let writable = std::cmp::min(remaining, (file_info.length - offset_in_file) as usize);
            if writable == 0 {
                continue;
            }

            if file_idx >= priorities.len() || priorities[file_idx] != FilePriority::Skip {
                let mut file = self.get_write_file(&file_info.path)?;
                file.seek(SeekFrom::Start(offset_in_file))?;
                file.write_all(&data[data_offset..data_offset + writable])?;
                let _ = file.sync_data();
            }

            data_offset += writable;
            remaining -= writable;
            if remaining == 0 {
                break;
            }
        }

        self.read_cache.lock().pop(&index);
        Ok(())
    }

    pub fn read_piece(&self, index: u32, piece_size: u64) -> Result<Arc<Vec<u8>>> {
        {
            let mut cache = self.read_cache.lock();
            if let Some(data) = cache.get(&index) {
                return Ok(data.clone());
            }
        }

        let data = self.read_piece_uncached(index, piece_size)?;
        let arc = Arc::new(data);

        self.read_cache.lock().put(index, arc.clone());

        Ok(arc)
    }

    fn read_piece_uncached(&self, index: u32, piece_size: u64) -> Result<Vec<u8>> {
        let piece_offset = index as u64 * self.meta.piece_length;
        let mut result = vec![0u8; piece_size as usize];
        let mut data_offset = 0usize;
        let mut remaining = piece_size as usize;

        for file_info in &self.meta.files {
            let file_start = file_info.offset;
            let file_end = file_info.offset + file_info.length;

            let abs_start = piece_offset + data_offset as u64;
            if abs_start >= file_end || abs_start + remaining as u64 <= file_start {
                continue;
            }

            let offset_in_file = abs_start.saturating_sub(file_start);
            let readable = std::cmp::min(remaining, (file_info.length - offset_in_file) as usize);
            if readable == 0 {
                continue;
            }

            if readable >= 1_048_576 {
                self.mmap_read(
                    &file_info.path,
                    offset_in_file,
                    &mut result[data_offset..data_offset + readable],
                )?;
            } else {
                let mut file = self.open_file_read(&file_info.path)?;
                file.seek(SeekFrom::Start(offset_in_file))?;
                file.read_exact(&mut result[data_offset..data_offset + readable])?;
            }

            data_offset += readable;
            remaining -= readable;
            if remaining == 0 {
                break;
            }
        }

        Ok(result)
    }

    fn mmap_read(&self, rel_path: &str, offset: u64, buf: &mut [u8]) -> Result<()> {
        let full_path = self.base_path.join(rel_path);
        let file = File::open(&full_path).map_err(|e| TorrentError::Storage(e.to_string()))?;

        let mmap = unsafe {
            MmapOptions::new()
                .offset(offset)
                .len(buf.len())
                .map(&file)
                .map_err(|e| TorrentError::Storage(format!("mmap: {}", e)))?
        };
        buf.copy_from_slice(&mmap);
        Ok(())
    }

    pub fn delete_files(&self) -> Result<()> {
        for file_info in &self.meta.files {
            let full_path = self.base_path.join(&file_info.path);
            if full_path.exists() {
                let canonical = full_path
                    .canonicalize()
                    .map_err(|e| TorrentError::Storage(format!("canonicalize: {}", e)))?;
                if !canonical.starts_with(&self.base_path) {
                    tracing::warn!("Skipping file outside base path: {:?}", full_path);
                    continue;
                }
                fs::remove_file(&canonical).map_err(|e| TorrentError::Storage(e.to_string()))?;
            }
        }
        let canonical_base = self
            .base_path
            .canonicalize()
            .unwrap_or(self.base_path.clone());
        let mut dirs: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();
        for file_info in &self.meta.files {
            if let Some(parent) = self.base_path.join(&file_info.path).parent()
                && parent != canonical_base
                    && let Ok(canonical_parent) = parent.canonicalize()
                        && canonical_parent.starts_with(&canonical_base) {
                            dirs.insert(canonical_parent);
                        }
        }
        let mut sorted: Vec<PathBuf> = dirs.into_iter().collect();
        sorted.sort_by(|a, b| b.components().count().cmp(&a.components().count()));
        for dir in sorted {
            let _ = fs::remove_dir(&dir);
        }
        Ok(())
    }
}
