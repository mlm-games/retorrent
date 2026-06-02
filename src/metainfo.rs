use crate::bencode::{BencodeParser, BencodeValue, extract_info_raw};
use crate::error::{Result, TorrentError};
use crate::types::InfoHash;
use sha1::{Digest, Sha1};

#[derive(Debug, Clone)]
pub struct MetaInfo {
    pub info_hash: InfoHash,
    pub name: String,
    pub piece_length: u64,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<FileInfo>,
    pub total_size: u64,
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub url_list: Vec<String>,
    #[allow(dead_code)]
    pub comment: Option<String>,
    #[allow(dead_code)]
    pub created_by: Option<String>,
    #[allow(dead_code)]
    pub creation_date: Option<i64>,
    #[allow(dead_code)]
    pub is_private: bool,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub length: u64,
    pub offset: u64,
}

impl MetaInfo {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let info_raw = extract_info_raw(data)?;
        let info_hash = {
            let mut hasher = Sha1::new();
            hasher.update(info_raw);
            let result = hasher.finalize();
            let mut hash = [0u8; 20];
            hash.copy_from_slice(&result);
            InfoHash(hash)
        };

        let root = BencodeParser::parse(data)?;
        let root_dict = root
            .as_dict()
            .ok_or_else(|| TorrentError::InvalidMetaInfo("Root not a dict".to_string()))?;

        let announce = root_dict
            .get("announce")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let announce_list = Self::parse_announce_list(root_dict.get("announce-list"));
        let url_list = Self::parse_string_list(root_dict.get("url-list"));

        let comment = root_dict
            .get("comment")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let created_by = root_dict
            .get("created by")
            .and_then(|v| v.as_string())
            .map(|s| s.to_string());

        let creation_date = root_dict.get("creation date").and_then(|v| v.as_integer());

        let info = root_dict
            .get("info")
            .ok_or_else(|| TorrentError::InvalidMetaInfo("Missing info dict".to_string()))?;

        let info_dict = info
            .as_dict()
            .ok_or_else(|| TorrentError::InvalidMetaInfo("info not a dict".to_string()))?;

        let name = info_dict
            .get("name")
            .and_then(|v| v.as_string())
            .unwrap_or("unknown")
            .to_string();

        let piece_length = info_dict
            .get("piece length")
            .and_then(|v| v.as_integer())
            .ok_or_else(|| TorrentError::InvalidMetaInfo("Missing piece length".to_string()))?
            as u64;

        let pieces_raw = info_dict
            .get("pieces")
            .and_then(|v| v.as_bytes())
            .ok_or_else(|| TorrentError::InvalidMetaInfo("Missing pieces".to_string()))?;

        if pieces_raw.len() % 20 != 0 {
            return Err(TorrentError::InvalidMetaInfo(
                "Invalid pieces length".to_string(),
            ));
        }

        let pieces: Vec<[u8; 20]> = pieces_raw
            .chunks_exact(20)
            .map(|chunk| {
                let mut hash = [0u8; 20];
                hash.copy_from_slice(chunk);
                hash
            })
            .collect();

        let is_private = info_dict
            .get("private")
            .and_then(|v| v.as_integer())
            .map(|v| v == 1)
            .unwrap_or(false);

        let (files, total_size) = if let Some(files_val) = info_dict.get("files") {
            Self::parse_multi_file(files_val, &name)?
        } else {
            let length = info_dict
                .get("length")
                .and_then(|v| v.as_integer())
                .ok_or_else(|| TorrentError::InvalidMetaInfo("Missing length".to_string()))?
                as u64;

            (
                vec![FileInfo {
                    path: name.clone(),
                    length,
                    offset: 0,
                }],
                length,
            )
        };

        Ok(MetaInfo {
            info_hash,
            name,
            piece_length,
            pieces,
            files,
            total_size,
            announce,
            announce_list,
            url_list,
            comment,
            created_by,
            creation_date,
            is_private,
        })
    }

    fn parse_multi_file(files_val: &BencodeValue, base_name: &str) -> Result<(Vec<FileInfo>, u64)> {
        let files_list = files_val
            .as_list()
            .ok_or_else(|| TorrentError::InvalidMetaInfo("files not a list".to_string()))?;

        let mut files = Vec::new();
        let mut offset = 0u64;

        for file_val in files_list {
            let file_dict = file_val
                .as_dict()
                .ok_or_else(|| TorrentError::InvalidMetaInfo("file not a dict".to_string()))?;

            let length = file_dict
                .get("length")
                .and_then(|v| v.as_integer())
                .ok_or_else(|| TorrentError::InvalidMetaInfo("Missing file length".to_string()))?
                as u64;

            let path_list = file_dict
                .get("path")
                .and_then(|v| v.as_list())
                .ok_or_else(|| TorrentError::InvalidMetaInfo("Missing file path".to_string()))?;

            let path_parts: Vec<&str> = path_list.iter().filter_map(|v| v.as_string()).collect();

            let mut full_path = base_name.to_string();
            for part in &path_parts {
                if part.contains("..")
                    || part.contains('/')
                    || part.contains('\\')
                    || part.contains('\0')
                {
                    return Err(TorrentError::InvalidMetaInfo(
                        "Invalid path component in torrent file".to_string(),
                    ));
                }
                full_path.push('/');
                full_path.push_str(part);
            }

            files.push(FileInfo {
                path: full_path,
                length,
                offset,
            });
            offset += length;
        }

        Ok((files, offset))
    }

    fn parse_announce_list(val: Option<&BencodeValue>) -> Vec<Vec<String>> {
        val.and_then(|v| v.as_list())
            .map(|tiers| {
                tiers
                    .iter()
                    .filter_map(|tier| {
                        tier.as_list().map(|urls| {
                            urls.iter()
                                .filter_map(|u| u.as_string().map(|s| s.to_string()))
                                .collect()
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Parse BEP-19 `url-list` (a list of strings) and the older
    /// `httpseeds` field. Webseed URLs are HTTP(S) endpoints from which
    /// pieces can be fetched with `Range` requests.
    fn parse_string_list(val: Option<&BencodeValue>) -> Vec<String> {
        val.and_then(|v| v.as_list())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|u| u.as_string().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Build the webseed URL for a given file, per BEP-19.
    /// `base` is one of the entries from `url-list` or `httpseeds`.
    /// For a single-file torrent, the file path is just `info.name`
    /// (and `file_path` will equal `self.name`).
    /// For multi-file torrents, `file_path` already includes the
    /// `info.name` prefix (the way `parse_multi_file` constructs it).
    pub fn webseed_url_for(&self, base: &str, file_path: &str) -> String {
        let base = if base.ends_with('/') {
            base.to_string()
        } else {
            format!("{}/", base)
        };
        // file_path is the torrent-relative file path. We append it
        // directly to the base URL. For single-file torrents,
        // file_path == self.name. For multi-file torrents, file_path
        // is "{info.name}/{rest}" already.
        format!("{}{}", base, file_path)
    }

    pub fn num_pieces(&self) -> u32 {
        self.pieces.len() as u32
    }

    #[allow(dead_code)]
    pub fn piece_size(&self, index: u32) -> u64 {
        if index as u64 == (self.pieces.len() as u64 - 1) {
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
}
