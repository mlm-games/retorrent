use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub download_dir: PathBuf,
    pub max_connections: usize,
    pub max_connections_per_torrent: usize,
    pub max_upload_rate: u64,
    pub max_download_rate: u64,
    pub listen_port: u16,
    pub dht_enabled: bool,
    pub pex_enabled: bool,
    pub enable_encryption: bool,
    pub cache_size_mb: usize,
    pub prealloc_files: bool,
    pub endgame_mode: bool,
    pub accept_incoming: bool,
    pub choke_interval: u64,
    pub upload_slots: usize,
    pub optimistic_unchoke_interval: u64,
    pub seed_ratio_limit: f64,
    pub seed_ratio_enabled: bool,
    pub auto_resume: bool,
    pub pipeline_depth: u32,
}

impl Default for Config {
    fn default() -> Self {
        let download_dir = dirs::download_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("Retorrent");

        Self {
            download_dir,
            max_connections: 800,
            max_connections_per_torrent: 150,
            max_upload_rate: 0,
            max_download_rate: 0,
            listen_port: 6881,
            dht_enabled: true,
            pex_enabled: true,
            enable_encryption: false,
            cache_size_mb: 256,
            prealloc_files: true,
            endgame_mode: true,
            accept_incoming: true,
            choke_interval: 10,
            upload_slots: 4,
            optimistic_unchoke_interval: 30,
            seed_ratio_limit: 2.0,
            seed_ratio_enabled: false,
            auto_resume: true,
            pipeline_depth: 32,
        }
    }
}

impl Config {
    pub fn load_or_default() -> Self {
        let config_path = Self::config_path();
        if config_path.exists() {
            match std::fs::read_to_string(&config_path) {
                Ok(data) => match serde_json::from_str(&data) {
                    Ok(config) => return config,
                    Err(e) => tracing::warn!(
                        "Failed to parse config at {:?}: {}. Using defaults.",
                        config_path,
                        e
                    ),
                },
                Err(e) => tracing::warn!(
                    "Failed to read config at {:?}: {}. Using defaults.",
                    config_path,
                    e
                ),
            }
        }
        let config = Config::default();
        let _ = config.save();
        config
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let config_path = Self::config_path();
        if let Some(parent) = config_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let data = serde_json::to_string_pretty(self)?;
        std::fs::write(config_path, data)?;
        Ok(())
    }

    fn config_path() -> PathBuf {
        dirs::config_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("retorrent")
            .join("config.json")
    }

    pub fn resume_dir() -> PathBuf {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("retorrent")
            .join("resume")
    }
}